package kv

import (
	"encoding/json"
	"testing"
)

func TestStore_PutGetDelete(t *testing.T) {
	s := NewStore()
	if _, ok := s.Get("a"); ok {
		t.Fatal("空状态机不应有 key")
	}
	if _, err := s.Apply(EncodePut("a", "1")); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q %v, want 1 true", v, ok)
	}
	if _, err := s.Apply(EncodePut("a", "2")); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get("a"); v != "2" {
		t.Fatalf("覆盖后 Get(a) = %q, want 2", v)
	}
	if _, err := s.Apply(EncodeDelete("a")); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("删除后 Get(a) 应不存在")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
}

// TestStore_ReadCommand：线性化读是 no-op，只返回应用那一刻的值，
// 不改数据；读不存在的 key 返回 found=false。
func TestStore_ReadCommand(t *testing.T) {
	s := NewStore()
	res, err := s.Apply(EncodeRead("a"))
	if err != nil {
		t.Fatal(err)
	}
	var r ApplyResult
	if err := json.Unmarshal(res, &r); err != nil || r.Found {
		t.Fatalf("读不存在的 key 应 found=false，实际 %q %v", res, err)
	}
	if _, err := s.Apply(EncodePut("a", "7")); err != nil {
		t.Fatal(err)
	}
	res, err = s.Apply(EncodeRead("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(res, &r); err != nil || !r.Found || r.Value != "7" {
		t.Fatalf("读已存在的 key 应 found=true value=7，实际 %q %v", res, err)
	}
	if s.Len() != 1 {
		t.Fatalf("读不应改数据，Len = %d, want 1", s.Len())
	}
}

func TestStore_ApplyErrors(t *testing.T) {
	s := NewStore()
	if _, err := s.Apply([]byte("not json")); err == nil {
		t.Fatal("非法 JSON 命令应报错")
	}
	if _, err := s.Apply([]byte(`{"op":"wat","key":"a"}`)); err == nil {
		t.Fatal("未知 op 应报错")
	}
	if _, err := DecodeCommand([]byte("x")); err == nil {
		t.Fatal("DecodeCommand 对非法 JSON 应报错")
	}
}

func TestCommand_RoundTrip(t *testing.T) {
	c, err := DecodeCommand(EncodePut("k", "v"))
	if err != nil || c.Op != "put" || c.Key != "k" || c.Value != "v" {
		t.Fatalf("Put 编解码不对：%+v %v", c, err)
	}
	c, err = DecodeCommand(EncodeDelete("k"))
	if err != nil || c.Op != "delete" || c.Key != "k" {
		t.Fatalf("Delete 编解码不对：%+v %v", c, err)
	}
}

// TestStore_SnapshotRestore：快照→新状态机恢复→内容完全一致；
// 恢复坏数据应报错；空状态机的快照是 "{}"，不是空字节。
func TestStore_SnapshotRestore(t *testing.T) {
	s := NewStore()
	if _, err := s.Apply(EncodePut("a", "1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(EncodePut("b", "2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(EncodeDelete("b")); err != nil {
		t.Fatal(err)
	}
	data := s.Snapshot()
	if len(data) == 0 {
		t.Fatal("快照不应是空字节")
	}
	s2 := NewStore()
	if err := s2.Restore(data); err != nil {
		t.Fatal(err)
	}
	if got := s2.All(); len(got) != 1 || got["a"] != "1" {
		t.Fatalf("恢复后内容不对：%v", got)
	}
	// 恢复后继续写，不影响原状态机。
	if _, err := s2.Apply(EncodePut("c", "3")); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("c"); ok {
		t.Fatal("恢复后的写不应影响原状态机")
	}
	// 坏数据拒绝恢复。
	if err := NewStore().Restore([]byte("not json")); err == nil {
		t.Fatal("非法 JSON 快照应报错")
	}
	// 空状态机快照。
	if got := NewStore().Snapshot(); string(got) != "{}" {
		t.Fatalf("空状态机快照应是 {}，实际 %q", got)
	}
	// 快照字节是确定的：同样的内容两次编出来必须相同
	// （模拟器确定性与快照对比都靠这一点）。
	if string(data) != string(s.Snapshot()) {
		t.Fatal("同样内容的快照两次编码不一致")
	}
}
