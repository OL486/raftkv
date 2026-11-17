// Package linearizability 用 Porcupine 做模型检查，
// 验证一段 KV 操作历史是否线性一致。
//
// 模型很简单：状态就是 map[string]string。
// Put(k, v) 置值返回成功；Read(k) 返回当前值或不存在。
// 只有线性化读（走 Raft 日志的 Read 命令）才能进历史：
// 读 follower 的 /kv/get 可能读到旧数据，天生不是线性化的，
// 拿它去检查会误报——见 docs/03-testing.md。
//
// 性能：历史按 key 分区（不同 key 的操作互不影响，
// 线性一致当且仅当每个 key 的投影都线性一致——
// 这是 Porcupine 文档里给 KV 举的例子）；
// 每 key 的操作数少了，指数级的检查才跑得动。
package linearizability

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/OL486/raftkv/internal/sim"
)

// PutInput 是一次 Put 操作的输入。
type PutInput struct {
	Key   string
	Value string
}

// ReadInput 是一次线性化读操作的输入。
type ReadInput struct {
	Key string
}

// PutOutput 是一次 Put 操作的输出。
type PutOutput struct {
	OK bool
}

// ReadOutput 是一次线性化读操作的输出。
type ReadOutput struct {
	Value string
	Found bool
}

// Model 返回 KV 的 Porcupine 模型。
func Model() porcupine.Model {
	return porcupine.Model{
		Partition:         partitionByKey,
		Init:              func() interface{} { return map[string]string{} },
		Step:              step,
		Equal:             equalState,
		Hash:              hashState,
		DescribeOperation: describeOperation,
		DescribeState:     describeState,
	}
}

// step 是模型的单步转移：给定状态、输入、输出，
// 判断这一步是否可能，并返回新状态。必须是纯函数：
// 不修改传入的 state（Put 时拷贝一份 map）。
func step(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
	m := state.(map[string]string)
	switch in := input.(type) {
	case PutInput:
		out, ok := output.(PutOutput)
		if !ok {
			return false, nil
		}
		if !out.OK {
			// 失败的 Put 按“没生效”处理。注意：这只是兜底——
			// 失败的 Put 实际上可能日后提交（重试的命令已在日志里），
			// 那时“没生效”的建模会误报。所以调用方必须保证
			// 历史里没有失败的操作（Check 会先检查并拒绝），
			// 这个分支在正常测试里走不到。
			return true, m
		}
		next := make(map[string]string, len(m)+1)
		for k, v := range m {
			next[k] = v
		}
		next[in.Key] = in.Value
		return true, next
	case ReadInput:
		out, ok := output.(ReadOutput)
		if !ok {
			return false, nil
		}
		v, present := m[in.Key]
		if out.Found {
			if !present || v != out.Value {
				return false, nil
			}
		} else if present {
			return false, nil
		}
		return true, m
	}
	return false, nil
}

// partitionByKey 按 key 把历史分区：不同 key 的操作互不影响。
func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := make(map[string][]porcupine.Operation)
	var keys []string
	for _, op := range history {
		k := opKey(op.Input)
		if _, ok := byKey[k]; !ok {
			keys = append(keys, k)
		}
		byKey[k] = append(byKey[k], op)
	}
	sort.Strings(keys)
	out := make([][]porcupine.Operation, 0, len(keys))
	for _, k := range keys {
		out = append(out, byKey[k])
	}
	return out
}

// opKey 取一个操作的 key（分区用）。
func opKey(input interface{}) string {
	switch in := input.(type) {
	case PutInput:
		return in.Key
	case ReadInput:
		return in.Key
	}
	return ""
}

// equalState 比较两个状态（map）是否相等。
func equalState(s1, s2 interface{}) bool {
	m1, m2 := s1.(map[string]string), s2.(map[string]string)
	if len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		if m2[k] != v {
			return false
		}
	}
	return true
}

// hashState 是状态的哈希（FNV-1a，key 按字典序拼起来算）。
// Equal 的状态哈希一定相同。
func hashState(s interface{}) uint64 {
	m := s.(map[string]string)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := uint64(14695981039346656037)
	for _, k := range keys {
		for _, b := range []byte(k + "=" + m[k] + ";") {
			h ^= uint64(b)
			h *= 1099511628211
		}
	}
	return h
}

func describeOperation(input interface{}, output interface{}) string {
	switch in := input.(type) {
	case PutInput:
		return fmt.Sprintf("Put(%q=%q) -> %v", in.Key, in.Value, output)
	case ReadInput:
		return fmt.Sprintf("Read(%q) -> %v", in.Key, output)
	}
	return fmt.Sprintf("%v -> %v", input, output)
}

func describeState(state interface{}) string {
	m := state.(map[string]string)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%q=%q", k, m[k]))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// checkTimeout 是单次检查的超时：分区后每 key 的操作数不多，
// 正常几秒内就能出结果；超时返回 Unknown，按失败处理
// （并写明是超时不是 Illegal）。
const checkTimeout = 60 * time.Second

// Check 验证一段历史是否线性一致。
// 历史里不能有失败的操作（见 step 里 PutOutput{OK:false} 的注释）：
// 有失败操作时直接返回不通过并写明原因，不调检查器。
// 返回（是否通过，失败时的诊断文本）；通过时诊断为空。
func Check(history []porcupine.Operation) (bool, string) {
	for i, op := range history {
		if out, ok := op.Output.(PutOutput); ok && !out.OK {
			return false, fmt.Sprintf("历史第 %d 条是失败的 Put（%v），无法检查：失败的操作可能日后提交，模型按“没生效”处理会误报", i, op.Input)
		}
	}
	result, info := porcupine.CheckOperationsVerbose(Model(), history, checkTimeout)
	switch result {
	case porcupine.Ok:
		return true, ""
	case porcupine.Illegal:
		return false, formatIllegal(history, info)
	default: // Unknown：超时
		return false, fmt.Sprintf("检查超时（>%v）：历史 %d 条操作，检查器没能在时限内出结果", checkTimeout, len(history))
	}
}

// formatIllegal 格式化一次不通过的诊断：
// 先给 Porcupine 找到的最大部分线性化（最小反例的上下文），
// 再附完整历史（供人工复核）。
func formatIllegal(history []porcupine.Operation, info porcupine.LinearizationInfo) string {
	var b strings.Builder
	b.WriteString("历史不是线性一致的。\n")
	partials := info.PartialLinearizationsOperations()
	for pi, partition := range partials {
		b.WriteString(fmt.Sprintf("分区 %d 的最大部分线性化（共 %d 组候选）：\n", pi, len(partition)))
		for i, lin := range partition {
			if i >= 3 {
				b.WriteString("  （只显示前 3 组）\n")
				break
			}
			b.WriteString(fmt.Sprintf("  候选 %d：", i))
			for _, op := range lin {
				b.WriteString(" " + describeOperation(op.Input, op.Output))
			}
			b.WriteString("\n")
		}
	}
	b.WriteString(fmt.Sprintf("完整历史（%d 条，Call/Return 为纳秒时间戳）：\n", len(history)))
	for i, op := range history {
		b.WriteString(fmt.Sprintf("  #%d 客户端%d [%d, %d] %s\n", i, op.ClientId, op.Call, op.Return, describeOperation(op.Input, op.Output)))
	}
	return b.String()
}

// FromSimRecords 把模拟器的并发运行记录转成 Porcupine 历史。
// Call/Return 是虚拟时间（纳秒数）：虚拟时钟只往前走，
// 每个操作的 Call 在调用时读、Return 在返回时读，
// 所以“先返回的操作一定先于后调用的操作”这一真实顺序
// 在虚拟时间戳上同样成立——Porcupine 可以直接用它当时间戳。
func FromSimRecords(records []sim.OpRecord) []porcupine.Operation {
	out := make([]porcupine.Operation, 0, len(records))
	for _, r := range records {
		op := porcupine.Operation{
			ClientId: r.ClientID,
			Call:     int64(r.Call),
			Return:   int64(r.Return),
		}
		if r.Kind == "read" {
			op.Input = ReadInput{Key: r.Key}
			op.Output = ReadOutput{Value: r.Value, Found: r.Found}
		} else {
			op.Input = PutInput{Key: r.Key, Value: r.Value}
			op.Output = PutOutput{OK: r.OK}
		}
		out = append(out, op)
	}
	return out
}
