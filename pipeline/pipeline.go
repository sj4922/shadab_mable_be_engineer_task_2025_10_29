package pipeline

import (
	"runtime"
	"sync"
)

// Stage is the core interface for a pipeline stage. It takes an input channel and returns an output channel, spawning goroutines as needed.
type Stage[I, O any] func(in <-chan I) <-chan O

// Chain composes two stages together.
func Chain[I, M, O any](s1 Stage[I, M], s2 Stage[M, O]) Stage[I, O] {
	return func(in <-chan I) <-chan O {
		return s2(s1(in))
	}
}

// MapStage applies a mapping function to each event.
func MapStage[T any](f func(T) T) Stage[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for val := range in {
				out <- f(val)
			}
		}()
		return out
	}
}

// FilterStage filters events based on a predicate.
func FilterStage[T any](f func(T) bool) Stage[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for val := range in {
				if f(val) {
					out <- val
				}
			}
		}()
		return out
	}
}

// ReduceStage transforms each event from T to R.
func ReduceStage[T, R any](f func(T) R) Stage[T, R] {
	return func(in <-chan T) <-chan R {
		out := make(chan R)
		go func() {
			defer close(out)
			for val := range in {
				out <- f(val)
			}
		}()
		return out
	}
}

// GenerateStage generates additional events from each input event, including the original.
func GenerateStage[T any](f func(T) []T) Stage[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for val := range in {
				for _, newVal := range f(val) {
					out <- newVal
				}
			}
		}()
		return out
	}
}

// IfStage routes events to one of two sub-pipelines based on a condition. Sub-pipelines must have the same output type.
func IfStage[T, U any](cond func(T) bool, thenStage, elseStage Stage[T, U]) Stage[T, U] {
	return func(in <-chan T) <-chan U {
		out := make(chan U)
		thenIn := make(chan T)
		elseIn := make(chan T)
		thenOut := thenStage(thenIn)
		elseOut := elseStage(elseIn)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for val := range thenOut {
				out <- val
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for val := range elseOut {
				out <- val
			}
		}()
		go func() {
			wg.Wait()
			close(out)
		}()
		go func() {
			for val := range in {
				if cond(val) {
					thenIn <- val
				} else {
					elseIn <- val
				}
			}
			close(thenIn)
			close(elseIn)
		}()
		return out
	}
}

// FanOutStage fans out input to multiple workers, each running a copy of the worker stage, and merges outputs.
func FanOutStage[I, O any](numWorkers int, worker Stage[I, O]) Stage[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O, numWorkers*10)
		var ins []chan I
		var outs []<-chan O
		for i := 0; i < numWorkers; i++ {
			inCh := make(chan I, numWorkers*10)
			ins = append(ins, inCh)
			outs = append(outs, worker(inCh))
		}
		var wg sync.WaitGroup
		for _, o := range outs {
			wg.Add(1)
			go func(ch <-chan O) {
				defer wg.Done()
				for val := range ch {
					out <- val
				}
			}(o)
		}
		go func() {
			wg.Wait()
			close(out)
		}()
		go func() {
			i := 0
			for val := range in {
				ins[i] <- val
				i = (i + 1) % numWorkers
			}
			for _, inCh := range ins {
				close(inCh)
			}
		}()
		return out
	}
}

// BatchStage batches input events into slices of size batchSize.
func BatchStage[T any](batchSize int) Stage[T, []T] {
	return func(in <-chan T) <-chan []T {
		out := make(chan []T)
		go func() {
			defer close(out)
			var batch = make([]T, 0, batchSize)
			for val := range in {
				batch = append(batch, val)
				if len(batch) == batchSize {
					out <- batch
					batch = make([]T, 0, batchSize)
				}
			}
			if len(batch) > 0 {
				out <- batch
			}
		}()
		return out
	}
}

// FlattenStage flattens batches back into individual events.
func FlattenStage[T any]() Stage[[]T, T] {
	return func(in <-chan []T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for batch := range in {
				for _, val := range batch {
					out <- val
				}
			}
		}()
		return out
	}
}

// ForEachStage applies the processor to each event in a batch.
func ForEachStage[T, R any](processor Stage[T, R]) Stage[[]T, []R] {
	return func(in <-chan []T) <-chan []R {
		out := make(chan []R, 10)
		go func() {
			defer close(out)
			for batch := range in {
				results := make([]R, 0, len(batch))
				batchIn := make(chan T, len(batch))
				batchOut := processor(batchIn)
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for r := range batchOut {
						results = append(results, r)
					}
				}()
				for _, val := range batch {
					batchIn <- val
				}
				close(batchIn)
				wg.Wait()
				out <- results
			}
		}()
		return out
	}
}

// ParallelPipeline adds batching and fanning out to a processor stage.
func ParallelPipeline[T, R any](processor Stage[T, R], numWorkers int, batchSize int) Stage[T, R] {
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}
	return Chain(
		BatchStage[T](batchSize),
		Chain(
			FanOutStage[[]T, []R](
				numWorkers,
				ForEachStage[T, R](processor),
			),
			FlattenStage[R](),
		),
	)
}

// Collect runs the stage on an input slice and collects the output into a slice (sink).
func Collect[I, O any](stage Stage[I, O], input []I) []O {
	in := make(chan I, len(input)/10)
	go func() {
		for _, v := range input {
			in <- v
		}
		close(in)
	}()
	out := stage(in)
	result := make([]O, 0, len(input))
	for v := range out {
		result = append(result, v)
	}
	if result == nil {
		return make([]O, 0)
	}
	return result
}
