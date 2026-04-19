package sync_tool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchTaskWaitsForAsyncTasks(t *testing.T) {
	batch := NewBatchTaskWithConcurrency(2)

	var count int32
	batch.AddAsyncTask(func(ctx BatchContext) {
		_ = ctx
		atomic.AddInt32(&count, 1)
	})
	batch.AddAsyncTask(func(ctx BatchContext) {
		_ = ctx
		atomic.AddInt32(&count, 1)
	})

	done := make(chan struct{})
	go func() {
		batch.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait should return after all async tasks complete")
	}

	if got := atomic.LoadInt32(&count); got != 2 {
		t.Fatalf("expected 2 completed tasks, got %d", got)
	}
}

func TestBatchTaskRecoversFromTaskPanic(t *testing.T) {
	batch := NewBatchTaskWithConcurrency(1)

	batch.AddAsyncTask(func(ctx BatchContext) {
		// _ = ctx
		panic("boom")
	})
	batch.AddAsyncTask(func(ctx BatchContext) {
		_ = ctx
	})

	done := make(chan struct{})
	go func() {
		batch.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait should not block when a task panics")
	}
}

func TestBatchTaskRunsSerialTasksAfterAsyncTasks(t *testing.T) {
	batch := NewBatchTaskWithConcurrency(2)

	var mu sync.Mutex
	order := make([]string, 0, 4)

	record := func(step string) {
		mu.Lock()
		order = append(order, step)
		mu.Unlock()
	}

	batch.AddAsyncTask(func(ctx BatchContext) {
		time.Sleep(80 * time.Millisecond)
		record("read-1")
		ctx.AddSerialTask(func() {
			record("write-1")
		})
	})
	batch.AddAsyncTask(func(ctx BatchContext) {
		time.Sleep(20 * time.Millisecond)
		record("read-2")
		ctx.AddSerialTask(func() {
			record("write-2")
		})
	})

	batch.Wait()

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()

	if len(got) != 4 {
		t.Fatalf("expected 4 executed tasks, got %d", len(got))
	}

	if got[0] != "read-2" || got[1] != "read-1" {
		t.Fatalf("expected async reads to finish before writes, got %v", got)
	}

	if got[2] != "write-2" || got[3] != "write-1" {
		t.Fatalf("expected serial writes to run in enqueue order after reads, got %v", got)
	}
}

func TestBatchTaskRecoversFromSerialTaskPanic(t *testing.T) {
	batch := NewBatchTaskWithConcurrency(1)

	var count int32
	batch.AddAsyncTask(func(ctx BatchContext) {
		ctx.AddSerialTask(func() {
			panic("serial boom")
		})
		ctx.AddSerialTask(func() {
			atomic.AddInt32(&count, 1)
		})
	})

	batch.Wait()

	if got := atomic.LoadInt32(&count); got != 1 {
		t.Fatalf("expected later serial task to continue after panic, got %d", got)
	}
}

func TestBatchTaskWaitRunsSerialTasksEvenWhenAsyncErrorsExist(t *testing.T) {
	batch := NewBatchTaskWithConcurrency(2)

	var count int32
	batch.AddAsyncTaskWE(func(ctx BatchContext) error {
		ctx.AddSerialTask(func() {
			atomic.AddInt32(&count, 1)
		})
		return errors.New("read failed")
	})
	batch.AddAsyncTask(func(ctx BatchContext) {
		ctx.AddSerialTask(func() {
			atomic.AddInt32(&count, 1)
		})
	})

	errs := batch.Wait()

	if got := atomic.LoadInt32(&count); got != 2 {
		t.Fatalf("expected serial tasks to run even with async errors, got %d", got)
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 async error, got %d", len(errs))
	}
}

func TestBatchTaskCollectsAllAsyncErrors(t *testing.T) {
	batch := NewBatchTaskWithConcurrency(8)

	const taskCount = 16
	for i := 0; i < taskCount; i++ {
		batch.AddAsyncTaskWE(func(ctx BatchContext) error {
			_ = ctx
			return errors.New("read failed")
		})
	}

	errs := batch.Wait()
	if len(errs) != taskCount {
		t.Fatalf("expected %d async errors, got %d", taskCount, len(errs))
	}
}
