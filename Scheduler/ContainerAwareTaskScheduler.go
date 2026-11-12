package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xuri/excelize/v2"
)

// -------------------- 工具函数 --------------------
// 计算全局并发度
func totalParallelism(containers []*Container) int {
	sum := 0
	for _, c := range containers {

		for _, t := range c.Running {
			sum += t.Parallelism
		}

	}
	return sum
}

// 全局损耗系数
func globalPenalty(totalParallelism int) float64 {
	//switch totalParallelism {
	//case 0, 1, 2, 3, 4, 5, 6:
	//	return 1.04
	//case 7:
	//	return 1.04
	//case 8:
	//	return 1.09
	//case 9:
	//	return 1.12
	//case 10:
	//	return 1.16
	//case 11:
	//	return 1.20
	//case 12:
	//	return 1.26
	//default:
	//	return 1.4
	//}
	return 1
}

// -------------------- 容器方法 --------------------
// 添加任务到容器
func (c *Container) AddTask(task RunningTask, containers []*Container) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if !c.LastUpdate.IsZero() {
		dt := now.Sub(c.LastUpdate).Seconds()
		//fmt.Printf("上一次更新是在%s，间隔了%f", c.LastUpdate, dt)
		if dt > 0 && len(c.Running) > 0 {
			totalP := 0
			for _, t := range c.Running {
				totalP += t.Parallelism
			}
			if totalP > 0 {
				penalty := globalPenalty(totalParallelism(containers) + task.Parallelism)
				for i := range c.Running {
					alloc := math.Min(
						float64(c.Running[i].Parallelism),
						float64(c.Cores)*float64(c.Running[i].Parallelism)/float64(totalP),
					)
					c.Running[i].RemainingWork -= dt * alloc / penalty
					if c.Running[i].RemainingWork < 0 {
						c.Running[i].RemainingWork = 0
					}
					//fmt.Printf("期间进行了%f,%f,%f的工作量dt * alloc / penalty\n", dt, alloc, penalty)
				}
			}
		}
	}

	// 加入新任务
	c.Running = append(c.Running, task)
	c.LastUpdate = now

	fmt.Printf("完成 addTask %d\n", task.ID)

	GlobalLogger.Write("完成 addTask %d\n", task.ID)
}

// 移除任务
func (c *Container) RemoveTask(id int, containers []*Container) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if !c.LastUpdate.IsZero() {
		dt := now.Sub(c.LastUpdate).Seconds()
		if dt > 0 && len(c.Running) > 0 {
			totalP := 0
			for _, t := range c.Running {
				totalP += t.Parallelism
			}
			if totalP > 0 {
				penalty := globalPenalty(totalParallelism(containers))
				for i := range c.Running {
					alloc := math.Min(
						float64(c.Running[i].Parallelism),
						float64(c.Cores)*float64(c.Running[i].Parallelism)/float64(totalP),
					)
					c.Running[i].RemainingWork -= dt * alloc / penalty
					if c.Running[i].RemainingWork < 0 {
						c.Running[i].RemainingWork = 0
					}
				}
			}
		}
	}

	// 移除目标任务
	for i, t := range c.Running {
		if t.ID == id {
			c.Running = append(c.Running[:i], c.Running[i+1:]...)
			break
		}
	}

	c.LastUpdate = now
	fmt.Printf("完成 removeTask %d\n", id)
	GlobalLogger.Write("完成 removeTask %d\n", id)
}

// 获取容器内任务并发度总和
func (c *Container) TotalParallelism() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, t := range c.Running {
		total += t.Parallelism
	}
	return total
}

// -------------------- 收益函数 --------------------

func (c *Container) PredictAllCompletionTimes(newTask RunningTask, now time.Time, containers []*Container) map[int]float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	penalty := 1.0
	tasks := append([]RunningTask{}, c.Running...)

	// 更新已有任务
	if !c.LastUpdate.IsZero() {
		dt := now.Sub(c.LastUpdate).Seconds()
		if dt > 0 {
			totalP := 0
			for _, t := range tasks {
				totalP += t.Parallelism
			}
			if totalP > 0 {
				for i := range tasks {
					alloc := math.Min(float64(tasks[i].Parallelism),
						float64(c.Cores)*float64(tasks[i].Parallelism)/float64(totalP))
					tasks[i].RemainingWork -= dt * alloc / penalty
					if tasks[i].RemainingWork < 0 {
						tasks[i].RemainingWork = 0
					}
				}
			}
		}
	}

	tasks = append(tasks, newTask)

	finishTimes := make(map[int]float64)
	clock := 0.0

	for len(tasks) > 0 {
		totalP := 0
		for _, t := range tasks {
			totalP += t.Parallelism
		}
		if totalP == 0 {
			break
		}
		alloc := make([]float64, len(tasks))
		for i, t := range tasks {
			alloc[i] = math.Min(float64(t.Parallelism),
				float64(c.Cores)*float64(t.Parallelism)/float64(totalP))
		}

		minTime := math.Inf(1)
		minIdx := -1
		for i, t := range tasks {
			if alloc[i] > 0 && t.RemainingWork > 0 {
				finish := t.RemainingWork / alloc[i] * penalty
				if finish < minTime {
					minTime = finish
					minIdx = i
				}
			}
		}
		if minIdx == -1 {
			break
		}
		//fmt.Println("本轮最早完成任务:", tasks[minIdx], "完成时间：", minTime)
		for i := range tasks {
			if tasks[i].RemainingWork > 0 {
				tasks[i].RemainingWork -= minTime * alloc[i] / penalty
				if tasks[i].RemainingWork < 0 {
					tasks[i].RemainingWork = 0
				}
			}
		}

		clock += minTime
		finishedID := tasks[minIdx].ID
		finishTimes[finishedID] = clock
		tasks = append(tasks[:minIdx], tasks[minIdx+1:]...)
	}
	return finishTimes
}

// predictRunningCompletionTimes 预测容器中已有任务的剩余完成时间（不加入新任务）。
// 通过并发度为 0 的占位任务复用同一预测模型，占位任务不参与资源分配。
func (c *Container) predictRunningCompletionTimes(now time.Time, containers []*Container) map[int]float64 {
	ghost := RunningTask{ID: -1}
	return c.PredictAllCompletionTimes(ghost, now, containers)
}

// -------------------- 任务管理 --------------------
func loadTasksFromExcel(filename string) []Task {
	f, err := excelize.OpenFile(filename)
	if err != nil {
		log.Fatal(err)
	}
	rows, _ := f.GetRows("Sheet1")

	var tasks []Task
	headers := rows[0]
	colIndex := make(map[string]int)
	for i, name := range headers {
		colIndex[name] = i
	}
	for i, row := range rows {
		if i == 0 {
			continue
		}
		var task Task
		fmt.Sscanf(row[colIndex["task_id"]], "%d", &task.ID)
		fmt.Sscanf(row[colIndex["arrival_time"]], "%f", &task.Arrival)
		fmt.Sscanf(row[colIndex["WorkloadSingleCore"]], "%f", &task.WorkloadSingleCore)
		fmt.Sscanf(row[colIndex["parallelism"]], "%d", &task.Parallelism)
		fmt.Sscanf(row[colIndex["Deadline"]], "%f", &task.Deadline)
		fmt.Sscanf(row[colIndex["WorkUnits"]], "%d", &task.WorkUnits)
		fmt.Sscanf(row[colIndex["Importance"]], "%f", &task.Importance)
		task.TightType = row[colIndex["TightType"]]

		if row[colIndex["Important"]] == "TRUE" || row[colIndex["Important"]] == "true" {
			task.Important = true
		}
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Arrival < tasks[j].Arrival })
	return tasks
}

// -------------------- 保存调度结果到 Excel --------------------
func saveResultsToExcel(results []ResultRecord, filename string) {
	out := excelize.NewFile()
	sheet := "Results"
	out.SetSheetName(out.GetSheetName(0), sheet)

	headers := []string{
		"TaskID", "Container", "Parallelism",
		"Arrival", "Deadline",
		"WorkloadSingleCore", "Predicted", "Duration",
		"WaitTime", "TotalElapsed",
		"Violated", "Important", "Importance", "TightType",
	}

	// 写表头
	for i, h := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		out.SetCellValue(sheet, cell, h)
	}

	// 写入数据
	for i, r := range results {
		row := i + 2
		out.SetCellValue(sheet, fmt.Sprintf("A%d", row), r.TaskID)
		out.SetCellValue(sheet, fmt.Sprintf("B%d", row), r.ContainerID)
		out.SetCellValue(sheet, fmt.Sprintf("C%d", row), r.Parallelism)
		out.SetCellValue(sheet, fmt.Sprintf("D%d", row), r.Arrival)
		out.SetCellValue(sheet, fmt.Sprintf("E%d", row), r.Deadline)
		out.SetCellValue(sheet, fmt.Sprintf("F%d", row), r.WorkloadCore)
		out.SetCellValue(sheet, fmt.Sprintf("G%d", row), r.Predicted)
		out.SetCellValue(sheet, fmt.Sprintf("H%d", row), r.Duration)
		out.SetCellValue(sheet, fmt.Sprintf("I%d", row), r.WaitTime)
		out.SetCellValue(sheet, fmt.Sprintf("J%d", row), r.TotalElapsed)
		out.SetCellValue(sheet, fmt.Sprintf("K%d", row), r.Violated)
		out.SetCellValue(sheet, fmt.Sprintf("L%d", row), r.Important)
		out.SetCellValue(sheet, fmt.Sprintf("M%d", row), r.Importance)
		out.SetCellValue(sheet, fmt.Sprintf("N%d", row), r.TightType) // 保存 TightType 字段

	}

	if err := out.SaveAs(filename); err != nil {
		out.SaveAs("副本" + filename)
		log.Fatal(err)

	}
	fmt.Printf("📊 结果已保存到 %s（共 %d 条记录）\n", filename, len(results))
	GlobalLogger.Write("📊 结果已保存到 %s（共 %d 条记录）\n", filename, len(results))
}

// -------------------- 调度逻辑 --------------------
// 周期性调度器：
// 每隔固定周期（100ms），检查当前任务池 pending，
// 从中选择收益最高的 (task, container) 对，执行一次调度。
// ✅ 调度器
func ContainerAwarescheduler(taskCh <-chan Task, containers []*Container, slackRatio float64, results *[]ResultRecord, mu *sync.Mutex, wg *sync.WaitGroup, doneCh chan struct{}) {

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	defer close(doneCh) // 调度器结束后通知主线程

	var pending []Task
	taskChClosed := false

	for range ticker.C {
		// Step1: 从通道取任务
	Loop:
		for {
			select {
			case t, ok := <-taskCh:
				if !ok {
					taskChClosed = true
					break Loop
				}
				pending = append(pending, t)
			default:
				break Loop
			}
		}
		if len(pending) > 0 {
			fmt.Printf(
				"🕐 调度周期状态 | 待调度任务数: %d | 通道剩余任务数: %d | 当前系统总并发度: %d\n", len(pending), len(taskCh), totalParallelism(containers),
			)
			GlobalLogger.Write("🕐 调度周期状态 | 待调度任务数: %d | 通道剩余任务数: %d | 当前系统总并发度: %d\n", len(pending), len(taskCh), totalParallelism(containers))
		}

		// Step2: 若任务全部调度完毕则退出
		if taskChClosed && len(pending) == 0 {
			fmt.Println("✅ 所有任务已调度完毕，退出调度器。")
			GlobalLogger.Write("✅ 所有任务已调度完毕，退出调度器。")
			return
		}

		if len(pending) == 0 {
			continue
		}

		// Step3: 正常调度逻辑
		bestScore := -1e9
		var bestTask Task
		var bestContainer *Container
		var bestDuration float64

		for _, t := range pending {
			for _, c := range containers {
				score, ok, duration := evaluateBenefit(c, t, time.Now(), slackRatio, containers)
				if !ok {
					//容器因特殊条件不满足
					continue
				}
				//当前容器总并发度
				currentParallel := c.TotalParallelism()
				//if score >= bestScore ||
				//	(math.Abs(score-bestScore) < 1e-6 && bestContainer != nil && currentParallel < bestContainer.TotalParallelism()) {
				//	bestScore = score
				//	bestTask = t
				//	bestContainer = c
				//	bestDuration = duration
				//}
				if score >= bestScore ||
					(math.Abs(score-bestScore) < 1e-6 && bestContainer != nil && c.Cores <= bestContainer.Cores && currentParallel <= bestContainer.TotalParallelism()) {
					bestScore = score
					bestTask = t
					bestContainer = c
					bestDuration = duration
				}
			}
		}

		if bestContainer != nil {
			for i, t := range pending {
				if t.ID == bestTask.ID {
					pending = append(pending[:i], pending[i+1:]...)
					break
				}
			}
			wg.Add(1)
			go dispatchTask(bestTask, bestContainer, bestDuration, containers, results, mu, wg)
			fmt.Printf("🚀 调度任务 %d -> 容器 %s | 得分=%.2f | 预测=%.2fs\n", bestTask.ID, bestContainer.ID, bestScore, bestDuration)
			GlobalLogger.Write("🚀 调度任务 %d -> 容器 %s | 得分=%.2f | 预测=%.2fs\n", bestTask.ID, bestContainer.ID, bestScore, bestDuration)
		} else {

		}
	}
}

func ContainerAwareschedulerWithProtection(taskCh <-chan Task, containers []*Container, slackRatio float64, results *[]ResultRecord, mu *sync.Mutex, wg *sync.WaitGroup, doneCh chan struct{}) {

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	defer close(doneCh) // 调度器结束后通知主线程

	var pending []Task
	var failedTasks []Task // 存储调度失败任务
	taskChClosed := false

	for range ticker.C {
		// Step 1: 从通道取任务
	Loop:
		for {
			select {
			case t, ok := <-taskCh:
				if !ok {
					taskChClosed = true
					break Loop
				}
				pending = append(pending, t)
			default:
				break Loop
			}
		}

		if len(pending) > 0 {
			fmt.Printf(
				"🕐 调度周期状态 | 待调度任务数: %d | 通道剩余任务数: %d | 当前系统总并发度: %d\n",
				len(pending), len(taskCh), totalParallelism(containers),
			)
			GlobalLogger.Write("🕐 调度周期状态 | 待调度任务数: %d | 通道剩余任务数: %d | 当前系统总并发度: %d\n",
				len(pending), len(taskCh), totalParallelism(containers))
		}

		// Step 2: 所有任务都处理完时退出主循环
		if taskChClosed && len(pending) == 0 {
			if len(failedTasks) > 0 {
				fmt.Printf("⚠️ 存在 %d 个调度失败任务，重新进入二次调度阶段...\n", len(failedTasks))
				pending = append(pending, failedTasks...)
				failedTasks = nil
				taskChClosed = false // 再开一轮
				continue
			}
			fmt.Println("✅ 所有任务均已完成调度。")
			GlobalLogger.Write("✅ 所有任务均已完成调度。")
			return
		}

		if len(pending) == 0 {
			continue
		}

		// Step 3: 调度逻辑
		bestScore := -1e9
		var bestTask Task
		var bestContainer *Container
		var bestDuration float64
		var bestBreakdown UtilityBreakdown
		stats := ActiveOverloadStats
		decisionStart := time.Now()

		for _, t := range pending {
			for _, c := range containers {
				b := evaluateUtilityWithProtection(c, t, time.Now(), slackRatio, containers)
				if stats != nil {
					stats.AddCandidate(t.ID, b)
				}
				if !b.Feasible {
					continue
				}
				//currentParallel := c.TotalParallelism()
				//if score > bestScore ||
				//	(math.Abs(score-bestScore) < 1e-6 && bestContainer != nil &&
				//		currentParallel < bestContainer.TotalParallelism()) {
				//	bestScore = score
				//	bestTask = t
				//	bestContainer = c
				//	bestDuration = duration
				//}
				if b.Total > bestScore ||
					(math.Abs(b.Total-bestScore) < 1e-6 && bestContainer != nil &&
						c.Cores <= bestContainer.Cores) {
					bestScore = b.Total
					bestTask = t
					bestContainer = c
					bestDuration = b.Predicted
					bestBreakdown = b
				}
			}
		}

		// Step 4: 调度结果处理
		if bestContainer != nil {
			for i, t := range pending {
				if t.ID == bestTask.ID {
					pending = append(pending[:i], pending[i+1:]...)
					break
				}
			}
			wg.Add(1)
			go dispatchTask(bestTask, bestContainer, bestDuration, containers, results, mu, wg)
			logScheduleDecision(bestTask.ID, bestContainer.ID, bestScore, bestBreakdown, bestDuration)
		} else {
			// 本轮所有任务无法调度，存入失败列表
			if stats != nil {
				stats.AddBlockedCycle(pending)
			}
			failedTasks = append(failedTasks, pending...)
			fmt.Printf("❌ 当前周期 %d 个任务均无法调度，暂存等待下一轮。\n", len(pending))
			GlobalLogger.Write("❌ 当前周期 %d 个任务均无法调度，暂存等待下一轮。\n", len(pending))
			pending = pending[:0]
		}

		if stats != nil {
			stats.AddCycleDecision(time.Since(decisionStart))
		}
	}
}

func ContainerAwareschedulerWithBatchProtection(taskCh <-chan Task, containers []*Container, slackRatio float64, results *[]ResultRecord, mu *sync.Mutex, wg *sync.WaitGroup, doneCh chan struct{}) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	defer close(doneCh)

	var pending []Task   // 待调度任务
	var failTasks []Task // 失败任务池
	taskChClosed := false

	for range ticker.C {
		// Step 1: 从通道取任务
	Loop:
		for {
			select {
			case t, ok := <-taskCh:
				if !ok {
					taskChClosed = true
					break Loop
				}
				pending = append(pending, t)
			default:
				break Loop
			}
		}

		activeList := pending
		source := "pending"

		// 如果 pending 空，则尝试调度失败任务
		if len(activeList) == 0 && len(failTasks) > 0 {
			activeList = failTasks
			source = "failTasks"
		}

		if len(activeList) > 0 {
			fmt.Printf("🕐 调度周期状态 | 任务源: %s | 待调度任务数: %d | 当前系统总并发度: %d\n",
				source, len(activeList), totalParallelism(containers))
			GlobalLogger.Write("🕐 调度周期状态 | 任务源: %s | 待调度任务数: %d | 当前系统总并发度: %d\n",
				source, len(activeList), totalParallelism(containers))
		}

		// Step 2: 判断是否结束
		if taskChClosed && len(pending) == 0 && len(failTasks) == 0 {
			fmt.Println("✅ 所有任务（含失败重调）已完成调度。")
			GlobalLogger.Write("✅ 所有任务（含失败重调）已完成调度。")
			return
		}

		if len(activeList) == 0 {
			continue
		}

		// Step 3: 调度逻辑
		bestScore := -1e9
		var bestTask Task
		var bestContainer *Container
		var bestDuration float64
		var bestIdx int = -1
		var bestBreakdown UtilityBreakdown

		for i, t := range activeList {
			for _, c := range containers {
				b := evaluateUtilityWithProtection(c, t, time.Now(), slackRatio, containers)
				if !b.Feasible {
					continue
				}
				currentParallel := c.TotalParallelism()
				if b.Total > bestScore ||
					(math.Abs(b.Total-bestScore) < 1e-6 && bestContainer != nil &&
						currentParallel < bestContainer.TotalParallelism()) {
					bestScore = b.Total
					bestTask = t
					bestContainer = c
					bestDuration = b.Predicted
					bestBreakdown = b
					bestIdx = i
				}
			}
		}

		// Step 4: 调度决策
		if bestContainer != nil {
			if source == "pending" {
				pending = append(pending[:bestIdx], pending[bestIdx+1:]...)
			} else {
				failTasks = append(failTasks[:bestIdx], failTasks[bestIdx+1:]...)
			}

			wg.Add(1)
			go func(t Task, c *Container, d float64) {
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("❌ 任务 %d 在容器 %s 上执行失败，重新加入失败队列。\n", t.ID, c.ID)
						GlobalLogger.Write("❌ 任务 %d 在容器 %s 上执行失败，重新加入失败队列。\n", t.ID, c.ID)
						mu.Lock()
						failTasks = append(failTasks, t)
						mu.Unlock()
					}
				}()
				dispatchTask(t, c, d, containers, results, mu, wg)
			}(bestTask, bestContainer, bestDuration)

			logScheduleDecision(bestTask.ID, bestContainer.ID, bestScore, bestBreakdown, bestDuration)
		} else {
			// 🧩 新增：如果没有任何任务分配成功
			if len(activeList) > 0 {
				mu.Lock()
				if source == "pending" {
					// pending 任务还未进入 failTasks，需要转入
					failTasks = append(failTasks, activeList...)
					pending = nil
				} else {
					// 来自 failTasks 的任务，直接保留原样，不需要重复加入
					fmt.Printf("⚠️ 无可分配容器，本轮 failTasks 保留 %d 个任务等待下次尝试。\n", len(failTasks))
				}
				mu.Unlock()

				GlobalLogger.Write("⚠️ 无可分配容器，本轮 %s 源任务暂未分配。\n", source)
			}
		}
	}
}

// dispatchTask: 将一个任务分配到指定容器执行，并记录执行结果
// ---------------------------------------------------------------------
// 输入：
//
//	task        - 要执行的任务
//	container   - 已选定的目标容器
//	containers  - 系统中所有容器（用于计算全局并发度）
//	results     - 全局结果记录切片指针
//	mu          - 保护 results 写入的互斥锁
//	wg          - WaitGroup，用于同步任务结束
func dispatchTask(task Task, container *Container, predicted float64, containers []*Container, results *[]ResultRecord, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done() // 调度结束后减少等待计数
	// 172.30.140.84 上的容器（C3/C4/C5）性能约为基准机的 1/1.1426394
	factor := 1.0
	if strings.Contains(container.URL, "172.30.140.84") {
		factor = 1.1426394
	}
	_ = factor // 预留：暂未参与计算，避免未使用变量编译错误
	// Step1: 构建运行态任务结构体（登记前的任务信息）
	newTask := RunningTask{
		ID:            task.ID,
		StartTime:     time.Now(),              // 实际开始时间
		Parallelism:   task.Parallelism,        // 并发度
		RemainingWork: task.WorkloadSingleCore, // 剩余计算量（单核执行时间）
		Predicted:     predicted,               // 暂定为理论预测时间，可替换为模型预测值
		Deadline:      task.Deadline,           // 截止时间
		Important:     task.Important,          // 是否为重要任务
		Importance:    task.Importance,         // 任务重要性权重
		TightType:     task.TightType,
	}

	// Step2: 在目标容器中登记任务（模拟资源占用）
	// 同时内部根据所有容器的总并发度计算损耗系数 penalty
	container.AddTask(newTask, containers)

	// Step2.1: 登记完成后重新预测容器内各任务的完成时间，并刷新其预测值
	container.PredictTimes(time.Now(), containers)

	// Step3: 通过 HTTP 向容器发送执行请求
	req := TaskRequest{
		WorkUnits:   int(float64(task.WorkUnits) * factor), // 实际计算量
		Parallelism: task.Parallelism,                      // 并发度
	}
	body, _ := json.Marshal(req)
	resp, err := http.Post(container.URL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("❌ 任务 %d -> 容器 %s 通信失败: %v\n", task.ID, container.ID, err)
		return
	}
	defer resp.Body.Close()

	// Step4: 解析容器返回的执行结果（执行时间）
	var result TaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("❌ 任务 %d -> 容器 %s 返回结果解析失败: %v\n", task.ID, container.ID, err)
		return
	}

	// Step5: 先取容器维护的最新预测时间（须在 RemoveTask 之前），再释放资源
	predictedExec := newTask.Predicted
	if p, ok := container.PredictedTime(task.ID); ok {
		predictedExec = p
	}
	container.RemoveTask(task.ID, containers)

	// Step6: 计算时间指标
	actualExec := result.Duration                          // 容器实际执行时间（不含等待）
	totalElapsed := time.Since(task.ArrivalTime).Seconds() // 从任务到达至完成的总耗时
	waitTime := totalElapsed - actualExec                  // 等待+调度时间
	violated := totalElapsed > 1.05*task.Deadline          // 判断是否超过 Deadline

	// Step7: 将调度结果写入全局记录
	mu.Lock()
	*results = append(*results, ResultRecord{
		TaskID:       task.ID,                 // 任务 ID
		ContainerID:  container.ID,            // 容器 ID
		Parallelism:  task.Parallelism,        // 并发度
		Arrival:      task.Arrival,            // 相对到达时间（秒）
		Deadline:     task.Deadline,           // 截止时间（秒）
		WorkloadCore: task.WorkloadSingleCore, // 单核工作量
		Predicted:    predictedExec,           // 预测执行时间
		Duration:     actualExec,              // 实际执行时间（容器返回）
		WaitTime:     waitTime,                // ⏱ 等待时间
		TotalElapsed: totalElapsed,            // ⏳ 从到达到完成总耗时
		Violated:     violated,                // 是否超时
		Forced:       false,                   // 是否强制分配
		Important:    task.Important,          // 是否重要任务
		Importance:   task.Importance,         // 重要性评分
		TightType:    task.TightType,
	})
	mu.Unlock()

	// Step8: 打印日志（带颜色输出）
	if violated {
		fmt.Printf("\033[31m❌ 任务 %d -> 容器 %s | 预计=%.2fs | 实际=%.2fs | Deadline=%.2fs | 超时！\033[0m\n", task.ID, container.ID, predictedExec, result.Duration, task.Deadline)
		GlobalLogger.Write("\033[31m❌ 任务 %d -> 容器 %s | 预计=%.2fs | 实际=%.2fs | Deadline=%.2fs | 超时！\033[0m\n", task.ID, container.ID, predictedExec, result.Duration, task.Deadline)
	} else {
		fmt.Printf("\033[32m✅ 任务 %d -> 容器 %s | 预计=%.2fs | 实际=%.2fs | Deadline=%.2fs | 按时完成\033[0m\n", task.ID, container.ID, predictedExec, result.Duration, task.Deadline)
		GlobalLogger.Write("\033[32m✅ 任务 %d -> 容器 %s | 预计=%.2fs | 实际=%.2fs | Deadline=%.2fs | 按时完成\033[0m\n", task.ID, container.ID, predictedExec, result.Duration, task.Deadline)
	}

}

// PredictTimes 重新预测容器内运行任务的完成时间，并刷新 Predicted（预测执行总时长）。
// 参考 updatePredictions 的处理方式：只更新仍在运行的任务；预测值无效（NaN/Inf/负数）时
// 记录日志并保留上一条有效预测，避免异常值污染后续调度判断。
func (c *Container) PredictTimes(now time.Time, containers []*Container) {
	finishTimes := c.predictRunningCompletionTimes(now, containers)

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.Running {
		rt := &c.Running[i]
		remaining, ok := finishTimes[rt.ID]
		if !ok {
			continue
		}
		if math.IsNaN(remaining) || math.IsInf(remaining, 0) || remaining < 0 {
			GlobalLogger.Write("⚠️ 任务 %d 的预测剩余时间无效：%v，保留上一条有效预测 %.2fs\n",
				rt.ID, remaining, rt.Predicted)
			continue
		}

		rt.LastPredictedRemaining = remaining
		rt.LastPredictionTime = now
		rt.Predicted = now.Sub(rt.StartTime).Seconds() + remaining
		rt.PredictionUpdates++
	}
}

// PredictedTime 返回容器中指定任务当前维护的预测执行总时长（秒）
func (c *Container) PredictedTime(id int) (float64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rt := range c.Running {
		if rt.ID == id {
			return rt.Predicted, true
		}
	}
	return 0, false
}

// -------------------- 任务分配至容器的收益计算 --------------------
// 默认权重（对比实验等场景可据此复位）
const (
	utilityW1Default = 0.5 // w1：任务重要性权重
	utilityW2Default = 1.0 // w2：性能收益权重
	utilityW3Default = 2.0 // w3：争用惩罚权重
)

// 效用模型尺度系数（U_ij = w1*I_i + w2*F_ij - w3*C_ij）。
// 作为变量而非常量提供，便于参数敏感性实验在每次运行前重新设定。
var (
	utilityW1 = utilityW1Default
	utilityW2 = utilityW2Default
	utilityW3 = utilityW3Default
)

// 效用模型形状/数值参数，取值使各项典型量级相当
const (
	utilityGamma   = 1.0  // γ：性能收益对 T_i^+/T_i^theo 的敏感度
	utilityLambdaD = 0.5  // λ_d：按时完成奖励
	utilityBeta    = 5.0  // β：延迟超过 slack 时的放大系数
	utilityEpsilon = 1e-6 // ε：防止除零

	// overloadRequireSelfTimeout 为 true 时沿用论文 O_ij 的定义：
	// 仅当新任务自身也超时，才把“拖慢运行任务至超时”判定为过载；
	// 为 false 时只要分配会导致运行任务超时就拒绝该候选。
	overloadRequireSelfTimeout = false
)

// normalizeImportance 将任务重要性归一化到 [0,1]
func normalizeImportance(v float64) float64 {
	return math.Min(1, math.Max(0, v/100))
}

// theoreticalMinDuration 任务在所有候选节点上的最短隔离执行时间 T_i^theo，
// 即工作量为单核工作量、并发度受节点核数限制时的最短执行时间
func theoreticalMinDuration(t Task, containers []*Container, fallback float64) float64 {
	best := math.Inf(1)
	for _, cont := range containers {
		alloc := math.Min(float64(t.Parallelism), float64(cont.Cores))
		if alloc <= 0 {
			continue
		}
		if d := t.WorkloadSingleCore / alloc; d < best {
			best = d
		}
	}
	if math.IsInf(best, 1) || best <= 0 {
		best = math.Max(fallback, 1e-9)
	}
	return best
}

// UtilityBreakdown 一次候选分配的效用分解，用于调度日志输出
type UtilityBreakdown struct {
	Total         float64 // U_ij = w1*I_i + w2*F_ij - w3*C_ij
	Importance    float64 // w1*I_i：重要性贡献
	Performance   float64 // w2*F_ij：性能效用贡献
	Contention    float64 // w3*C_ij：争用惩罚（越大越差）
	AffectedTasks int     // 争用惩罚影响到的运行任务个数
	TimeoutTasks  int     // 其中因争用由按时完成变为超时的运行任务个数
	Predicted     float64 // T_i^+：预测执行时间
	Feasible      bool    // 是否可分配（未触发过载保护）
	Overload      bool    // 是否因过载保护 O_ij 而被拒绝（用于统计）
}

// utilityOptions 控制效用计算中的机制开关，供消融实验（CAS-C / CAS-P / CAS-C-P）使用
type utilityOptions struct {
	// contentionPenalty 为 false 时等效 w3=0，不计入争用惩罚项
	contentionPenalty bool
	// overloadProtection 为 false 时不做过载保护，不因扩散超时而拒绝候选分配
	overloadProtection bool
	// quiet 为 true 时不输出过载保护日志。
	// 批次搜索（DP / 批内贪婪）单周期要评估十万级候选，日志会刷屏并拖慢搜索，
	// 因此这些场景传入 quiet=true；常规单步调度保持 false，行为不变。
	quiet bool
}

// defaultUtilityOptions 即论文模型的完整配置：争用惩罚与过载保护均启用
var defaultUtilityOptions = utilityOptions{contentionPenalty: true, overloadProtection: true}

// evaluateUtilityWithProtection 按完整效用模型评估把任务 t 分配到容器 c 的收益，
// 并用过载条件 O_ij 排除会向共运行任务扩散超时的候选分配
func evaluateUtilityWithProtection(c *Container, t Task, now time.Time, slackRatio float64, containers []*Container) UtilityBreakdown {
	return evaluateUtility(c, t, now, slackRatio, containers, defaultUtilityOptions)
}

// evaluateUtility 按给定机制开关评估候选分配的效用，计算方式与主模型完全一致，
// 仅按 opts 决定是否计入争用惩罚、是否启用过载保护
func evaluateUtility(c *Container, t Task, now time.Time, slackRatio float64, containers []*Container, opts utilityOptions) UtilityBreakdown {
	newTask := RunningTask{
		ID:            t.ID,
		StartTime:     now,
		Parallelism:   t.Parallelism,
		RemainingWork: t.WorkloadSingleCore,
		Deadline:      t.Deadline,
		Importance:    t.Importance,
		Important:     t.Important,
	}

	// 分配前 T_k 与分配后 T_k^+、T_i^+ 的完成时间预测，均以 now 为基准
	beforeFinish := c.predictRunningCompletionTimes(now, containers)
	afterFinish := c.PredictAllCompletionTimes(newTask, now, containers)

	duration, ok := afterFinish[t.ID]
	if !ok {
		return UtilityBreakdown{Total: -1e9}
	}

	// 1️⃣ 重要性效用 I_i：任务自身属性，不区分候选节点
	impScore := normalizeImportance(t.Importance)

	// 2️⃣ 性能效用：F_ij = 1/(1+γ·ln(T_i^+/T_i^theo)) + λ_d·1{T_i^+ ≤ δ_i}
	// theoretical := theoreticalMinDuration(t, containers, duration)
	theoretical := t.WorkloadSingleCore / float64(t.Parallelism)
	ratio := math.Max(1, duration/theoretical)
	performanceGain := 1 / (1 + utilityGamma*math.Log(ratio))
	// δ_i：以 now 为基准的剩余截止时间
	remainDeadline := t.Deadline - now.Sub(t.ArrivalTime).Seconds()
	if duration <= remainDeadline {
		performanceGain += utilityLambdaD
	}

	// 3️⃣ 过载保护：分配若会使运行任务由按时变为超时，则拒绝该候选（可按开关关闭）
	selfTimeout := duration > remainDeadline
	if opts.overloadProtection && (selfTimeout || !overloadRequireSelfTimeout) {
		for _, rt := range c.Running {
			tBefore, okBefore := beforeFinish[rt.ID]
			tAfter, okAfter := afterFinish[rt.ID]
			if !okBefore || !okAfter {
				continue
			}
			remainDeadlineK := rt.Deadline - now.Sub(rt.StartTime).Seconds()
			if tBefore <= remainDeadlineK && tAfter > remainDeadlineK {
				// if tAfter > remainDeadlineK {
				if !opts.quiet {
					GlobalLogger.Write("❌ 任务 %d -> 容器 %s 触发过载保护：任务 %d 预计由 %.2fs 拖慢至 %.2fs，超过剩余截止 %.2fs\n",
						t.ID, c.ID, rt.ID, tBefore, tAfter, remainDeadlineK)
				}
				return UtilityBreakdown{Total: -1e9, Predicted: duration, Overload: true}
			}
		}
	}

	// 4️⃣ 争用惩罚：C_ij = Σ_k I_k·g_k
	penalty := 0.0
	affectedTasks := 0
	timeoutTasks := 0
	for _, rt := range c.Running {
		tBefore, okBefore := beforeFinish[rt.ID]
		tAfter, okAfter := afterFinish[rt.ID]
		if !okBefore || !okAfter {
			continue
		}
		elapsedK := now.Sub(rt.StartTime).Seconds()
		deltaT := math.Max(0, tAfter-tBefore)      // ΔT_k：分配带来的额外延迟
		slackK := rt.Deadline - elapsedK - tBefore // s_k：分配前的截止时间余量
		scale := math.Abs(slackK) + utilityEpsilon
		g := deltaT / scale * (1 + utilityBeta*math.Max(0, deltaT-slackK)/scale)
		penalty += normalizeImportance(rt.Importance) * g
		if deltaT > 0 {
			affectedTasks++
		}
		// 分配前可按时完成、分配后超时：争用引发的超时
		if remainDeadlineK := rt.Deadline - elapsedK; tBefore <= remainDeadlineK && tAfter > remainDeadlineK {
			timeoutTasks++
		}
	}

	// 5️⃣ 综合效用及其分解（关闭争用惩罚时等效 w3=0）
	importance := utilityW1 * impScore
	performance := utilityW2 * performanceGain
	contention := 0.0
	if opts.contentionPenalty {
		contention = utilityW3 * penalty
	}

	return UtilityBreakdown{
		Total:         importance + performance - contention,
		Importance:    importance,
		Performance:   performance,
		Contention:    contention,
		AffectedTasks: affectedTasks,
		TimeoutTasks:  timeoutTasks,
		Predicted:     duration,
		Feasible:      true,
	}
}

// evaluateBenefitWithProtection 兼容旧调用方式：只返回效用值、是否可行与预测执行时间
func evaluateBenefitWithProtection(c *Container, t Task, now time.Time, slackRatio float64, containers []*Container) (float64, bool, float64) {
	b := evaluateUtilityWithProtection(c, t, now, slackRatio, containers)
	return b.Total, b.Feasible, b.Predicted
}

// logScheduleDecision 输出调度决策：总得分及其重要性、性能、争用惩罚分解
func logScheduleDecision(taskID int, containerID string, total float64, b UtilityBreakdown, duration float64) {
	fmt.Printf("🚀 调度任务 %d -> 容器 %s | 得分=%.2f | 重要性=%.3f | 性能=%.3f | 争用惩罚=%.3f (受影响任务数=%d, 其中争用超时=%d) | 预测=%.2fs\n",
		taskID, containerID, total, b.Importance, b.Performance, b.Contention, b.AffectedTasks, b.TimeoutTasks, duration)
	GlobalLogger.Write("🚀 调度任务 %d -> 容器 %s | 得分=%.2f | 重要性=%.3f | 性能=%.3f | 争用惩罚=%.3f (受影响任务数=%d, 其中争用超时=%d) | 预测=%.2fs\n",
		taskID, containerID, total, b.Importance, b.Performance, b.Contention, b.AffectedTasks, b.TimeoutTasks, duration)
}
func evaluateBenefit(c *Container, t Task, now time.Time, slackRatio float64, containers []*Container) (float64, bool, float64) {
	newTask := RunningTask{
		ID:            t.ID,
		StartTime:     now,
		Parallelism:   t.Parallelism,
		RemainingWork: t.WorkloadSingleCore,
		Deadline:      t.Deadline,
		Importance:    t.Importance,
		Important:     t.Important,
	}
	finishTimes := c.PredictAllCompletionTimes(newTask, now, containers)
	duration := finishTimes[t.ID]

	// 1️⃣ 固有重要性 Importance：归一化到 [0,1]
	impScore := math.Min(1, math.Max(0, t.Importance/100))

	// 2️⃣ 截止时间接近度收益（Deadline Proximity Gain）
	// ================================
	// 理论执行时间（单核工作量 / 并发度 * 1.4）
	theoretical := t.WorkloadSingleCore / float64(t.Parallelism) * 1.4
	deadlineGain := theoretical / t.Deadline
	// 越接近1越理想（小于1说明太快，大于1说明太慢）
	if deadlineGain > 1 {
		deadlineGain = 2 - deadlineGain // 超时反向惩罚（例如1.2→0.8）
	}
	deadlineGain = math.Min(1, math.Max(0, deadlineGain))

	// ================================
	// 3️⃣ 性能收益（Performance Gain）
	// ================================
	// 目标：越快完成（duration越短）得分越高；
	// 但过快执行（比理论时间快太多）也可轻微降低收益。
	TotalTtime := finishTimes[t.ID] + now.Sub(t.ArrivalTime).Seconds()
	// ✅ 统计当前系统中正在运行的重要任务数量

	performanceGain := 1 / (1 + math.Exp((duration/theoretical - 1))) // Sigmoid 映射
	// -> duration=theoretical 时 performanceGain≈0.5
	// -> duration < theoretical 时→趋近1；duration过长→趋近0
	performanceGain = math.Min(1, math.Max(0, performanceGain))
	if TotalTtime > 0.95*t.Deadline {
		performanceGain = performanceGain / 2
	}
	// 3. 负载均衡
	loadBefore := float64(c.TotalParallelism()) / float64(c.Cores)
	loadAfter := float64(c.TotalParallelism()+t.Parallelism) / float64(c.Cores)
	balance := 1.0 - math.Abs(loadAfter-loadBefore)

	// 4. 惩罚计算：评估把新任务分配到容器后，会让已有任务拖慢多少
	penalty := 0.0
	for _, rt := range c.Running {

		if ft, ok := finishTimes[rt.ID]; ok {
			// 1. 已经执行的时间（秒）
			elapsed := now.Sub(rt.StartTime).Seconds()
			// 2. absFinish = 已执行时间 + 预测剩余时间，表示该任务从开始到最终完成的总执行时长
			absFinish := elapsed + ft
			// 3. delay = 超时比例（相对Deadline）
			//    如果 absFinish > Deadline，则为正数，表示超时了多少
			//    如果 absFinish <= Deadline，则为负数或0，表示还能按时完成
			delay := (absFinish - rt.Deadline) / rt.Deadline
			fmt.Println(delay)
			// 4. 对“重要任务”的硬约束：
			//    如果任务被标记为重要，并且预计完成时间超过 slackRatio * Deadline
			//    就直接认为不可行（返回一个极低分数 -1e9）
			//fmt.Printf("任务%d分配时，预计任务%d执行时间为%f+%f,是否会超时%b,延迟delay=%f，absFinish=%f,Deadline=%f", t.ID, rt.ID, elapsed, absFinish, absFinish > slackRatio*rt.Deadline, delay, absFinish, rt.Deadline)
			if TotalTtime < t.Deadline {
				fmt.Printf("✅ 任务%d预计可完成", t.ID)
				if t.Important == true {
					if rt.Important && absFinish > 0.5*rt.Deadline {
						GlobalLogger.Write("任务%d分配可能会导致重要任务%d延迟", t.ID, rt.ID)
						penalty = penalty + 10
						//return -1e9, false, duration
					}
				}
				if t.Important == false {
					if rt.Important && absFinish > 0.5*rt.Deadline {
						GlobalLogger.Write("任务%d分配可能会导致重要任务%d延迟", t.ID, rt.ID)
						penalty = penalty + 10
						//return -1e9, false, duration
					}
					// 5. 普通惩罚：如果任务会超时（delay > 0）
					//    则根据任务的重要程度 Importance（浮点权重）加权叠加到 penalty
					if absFinish > slackRatio*rt.Deadline {
						//fmt.Println(t.ID, "会造成超时")
						//penalty += delay
						penalty = penalty + 0.1
						//return -1e9, false, duration
					}
				}
			}
			if TotalTtime > t.Deadline {
				fmt.Printf("❌任务%d预计不可可完成", t.ID)

				if rt.Important && absFinish > 0.5*rt.Deadline {
					GlobalLogger.Write("任务%d分配可能会导致重要任务%d延迟", t.ID, rt.ID)
					penalty = penalty + 10
					//return -1e9, false, duration
				}
				// 5. 普通惩罚：如果任务会超时（delay > 0）
				//    则根据任务的重要程度 Importance（浮点权重）加权叠加到 penalty
				if absFinish > slackRatio*rt.Deadline {
					//fmt.Println(t.ID, "会造成超时")
					//penalty += delay
					penalty = penalty + 0.1
					//return -1e9, false, duration
				}
			}

		}
	}

	// 控制 penalty 在 [0, 1] 范围内
	penalty = math.Min(1, penalty)

	score := impScore*0.5 + deadlineGain + performanceGain + 0*balance - penalty*5
	//performanceGain = 1 / duration
	//score := impScore*0.0 + deadlineGain*0.0 + performanceGain + 0*balance - penalty*0.0
	// 7️⃣ 输出日志
	fmt.Printf(
		"任务 %d 在容器 %s （当前并发:%d）评分如下：imp=%.2f, 截止时间收益=%.2f, 性能收益=%.2f, bal=%.2f, pen=%.2f, 总分=%.2f, 预计执行时间=%.2fs\n",
		t.ID, c.ID, c.TotalParallelism(),
		impScore, deadlineGain, performanceGain, balance, penalty, score, duration,
	)
	GlobalLogger.Write(
		"任务 %d 在容器 %s （当前并发:%d）评分如下：imp=%.2f, 截止时间收益=%.2f, 性能收益=%.2f, bal=%.2f, pen=%.2f, 总分=%.2f, 预计执行时间=%.2fs\n",
		t.ID, c.ID, c.TotalParallelism(),
		impScore, deadlineGain, performanceGain, balance, penalty, score, duration,
	)

	return score, true, duration
}
