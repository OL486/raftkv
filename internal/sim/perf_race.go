//go:build race

package sim

import "time"

// perfBudgetRace 是 -race 下 1000 步随机长跑的耗时上限。
// race 检测器给每次内存访问都加了检测，同一测试不带 -race 跑
// 只要约 0.6 秒，带 -race 要 10 秒上下——上限按非 race 标定
// （10 秒）时，-race 下的失败测的是检测器本身的开销，
// 不是模拟器的速度。-race 下放宽到 30 秒，
// 仍能抓住真正的性能退化（量级变化）。
const perfBudget = 30 * time.Second
