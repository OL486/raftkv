//go:build !race

package sim

import "time"

// perfBudget 是 1000 步随机长跑的耗时上限（不带 -race 时）。
// 实测约 0.6 秒（M3 基线 3.0 秒；M5 的 SaveMeta 与快速回退
// 反而让它更快了），10 秒上限留了十几倍余量。
const perfBudget = 10 * time.Second
