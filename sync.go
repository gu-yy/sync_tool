package sync_tool

import (
	"fmt"
	"log"
	"sync"
)

type BatchTask struct {
	taskChan chan struct{}
	wg       sync.WaitGroup

	serialMu    sync.Mutex
	serialTasks []func()

	errMu sync.Mutex
	errs  []error
}

func NewBatchTaskDefault() *BatchTask {
	// default 100 batch tasks
	return NewBatchTaskWithConcurrency(100)
}

func NewBatchTaskWithConcurrency(n int) *BatchTask {
	if n <= 0 {
		n = 1
	}

	task := &BatchTask{
		taskChan: make(chan struct{}, n),
	}
	return task
}

func (b *BatchTask) AddAsyncTask(task func(BatchContext)) *BatchTask {
	if task == nil {
		return nil
	}

	b.taskChan <- struct{}{}
	b.wg.Add(1)

	go func() {
		defer func() {
			<-b.taskChan
			b.wg.Done()

			// Prevent a panic inside one task from blocking the whole batch.
			if r := recover(); r != nil {
				log.Printf("async task panic: %v", r)
			}
		}()
		task(b)
	}()
	return b
}

func (b *BatchTask) AddAsyncTaskWE(task func(BatchContext) error) *BatchTask {
	if task == nil {
		return nil
	}
	b.taskChan <- struct{}{}
	b.wg.Add(1)
	go func() {
		var err error
		defer func() {
			<-b.taskChan
			b.wg.Done()
			if r := recover(); r != nil {
				log.Printf("async task panic: %v", r)
				err = fmt.Errorf("async task panic: %v", r)
			}
			if err != nil {
				b.appendErr(err)
			}
		}()
		err = task(b)
	}()
	return b
}

func (b *BatchTask) AddSerialTask(task func()) {
	if task == nil {
		return
	}

	b.serialMu.Lock()
	b.serialTasks = append(b.serialTasks, task)
	b.serialMu.Unlock()
}

func (b *BatchTask) Wait() []error {
	b.wg.Wait()

	b.serialMu.Lock()
	serialTasks := append([]func(){}, b.serialTasks...)
	b.serialTasks = nil
	b.serialMu.Unlock()

	for _, task := range serialTasks {
		func(task func()) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("serial task panic: %v", r)
					b.appendErr(fmt.Errorf("serial task panic: %v", r))
				}
			}()

			task()
		}(task)
	}

	return b.snapshotErrs()
}

func (b *BatchTask) appendErr(err error) {
	if err == nil {
		return
	}

	b.errMu.Lock()
	b.errs = append(b.errs, err)
	b.errMu.Unlock()
}

func (b *BatchTask) snapshotErrs() []error {
	b.errMu.Lock()
	defer b.errMu.Unlock()

	if len(b.errs) == 0 {
		return nil
	}

	return append([]error(nil), b.errs...)
}
