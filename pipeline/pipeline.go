// Package pipeline provides a composable, concurrent pipeline framework using channels and goroutines.
// It defines a Stage as a function transforming an input channel to an output channel, enabling
// the construction of complex data processing pipelines with parallelism and concurrency control.
//
// Example usage:
//
//	stage := Chain(
//	  MapStage[int](func(x int) int { return x * 2 }),
//	  FilterStage[int](func(x int) bool { return x > 5 }),
//	)
//	input := []int{1, 2, 3, 4, 5, 6}
//	result := Collect(stage, input)
//	fmt.Println(result) // Output: [6 8 10 12]
package pipeline

import (
	"sync"
)

// Hyperparameters for pipeline configuration.
var (
	NumWorkers     = getEnvAsIntOnly("PIPELINE_NUM_WORKERS", 4)
	BatchSize      = getEnvAsIntOnly("PIPELINE_BATCH_SIZE", 100)
	ChannelBufSize = getEnvAsIntOnly("PIPELINE_CHANNEL_BUF_SIZE", 10)
	CollectMinBuf  = getEnvAsIntOnly("PIPELINE_COLLECT_MIN_BUF", 10)
	CollectMaxBuf  = getEnvAsIntOnly("PIPELINE_COLLECT_MAX_BUF", 500)
)

// Stage is the core interface for a pipeline stage. It takes an input channel and returns an output channel, spawning goroutines as needed.
type Stage[I, O any] func(in <-chan I) <-chan O

// MapStage applies a mapping function to each event.
func MapStage[T any](f func(T) T) Stage[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T, ChannelBufSize)
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
		out := make(chan T, ChannelBufSize)
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
		out := make(chan R, ChannelBufSize)
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
		out := make(chan T, ChannelBufSize)
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
		out := make(chan O, numWorkers*ChannelBufSize)
		var ins []chan I
		var outs []<-chan O
		for i := 0; i < numWorkers; i++ {
			inCh := make(chan I, numWorkers*ChannelBufSize)
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
		out := make(chan []T, ChannelBufSize)
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
		out := make(chan T, ChannelBufSize)
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
		out := make(chan []R, ChannelBufSize)
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

// Chain composes two stages together.
//
// Example:
//
//	stage := Chain(
//	    MapStage[int](func(x int) int { return x * 2 }),
//	    FilterStage[int](func(x int) bool { return x > 5 }),
//	)
//	result := Collect(stage, []int{1, 2, 3, 4, 5, 6}) // [6, 8, 10, 12]
func Chain[I, M, O any](s1 Stage[I, M], s2 Stage[M, O]) Stage[I, O] {
	return func(in <-chan I) <-chan O {
		return s2(s1(in))
	}
}

// ParallelPipeline adds batching and fanning out to a processor stage.
//
// Example:
//
//	stage := ParallelPipeline[int, int](
//	    MapStage[int](func(x int) int { return x * 2 }),
//	    2, // number of workers
//	    10, // batch size
//	)
//	result := Collect(stage, []int{1, 2, 3, 4, 5, 6})
func ParallelPipeline[T, R any](processor Stage[T, R], numWorkers int, batchSize int) Stage[T, R] {
	if numWorkers == 0 {
		numWorkers = NumWorkers
	}
	if batchSize == 0 {
		batchSize = BatchSize
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
	bufferSize := max(min(CollectMaxBuf, len(input)/100), CollectMinBuf)
	in := make(chan I, bufferSize)
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
