package sync

type BatchTask struct {
	TaskChan chan int
}

func NewBatchTask() *BatchTask {
	return NewBatchTask()
}

func NewBatchTaskDefault() *BatchTask {
	// default 100 batch tasks
	return NewBatchTaskWithConcurrency(100)
}

func NewBatchTaskWithConcurrency(n int) *BatchTask {
	task := &BatchTask{
		TaskChan: make(chan int, n),
	}
	return task
}
