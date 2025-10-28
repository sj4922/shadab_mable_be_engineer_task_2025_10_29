// Package pipeline provides a generic streaming pipeline with stage composition,
// parallel processing, and fan-out/fan-in patterns.
//
// # Usage Examples
//
// ## Basic Stage Composition
//
//	type MyEvent struct {
//	    ID    int
//	    Value float64
//	    Name  string
//	}
//
//	// Create a simple transformation pipeline
//	mapStage := MapStage(func(e *MyEvent) *MyEvent {
//	    e.Value *= 2
//	    return e
//	})
//
//	filterStage := FilterStage(func(e *MyEvent) bool {
//	    return e.Value > 10
//	})
//
//	// Chain stages together
//	pipeline := Chain(mapStage, filterStage)
//
//	// Process events
//	events := []*MyEvent{
//	    {ID: 1, Value: 5.0, Name: "event1"},
//	    {ID: 2, Value: 15.0, Name: "event2"},
//	}
//
//	result := Collect(pipeline, events)
//	// result: [{ID: 2, Value: 30.0, Name: "event2"}]
//
// ## Conditional Routing
//
//	ifStage := IfStage(func(e *MyEvent) bool {
//	    return e.Value > 100
//	},
//	// High value processing
//	MapStage(func(e *MyEvent) *MyEvent {
//	    e.Name = "HIGH_" + e.Name
//	    return e
//	}),
//	// Normal value processing
//	MapStage(func(e *MyEvent) *MyEvent {
//	    e.Name = "NORMAL_" + e.Name
//	    return e
//	}))
//
// ## Parallel Processing
//
//	// CPU-intensive stage
//	heavyProcessor := MapStage(func(e *MyEvent) *MyEvent {
//	    // Simulate CPU-intensive work
//	    time.Sleep(10 * time.Millisecond)
//	    e.Value = math.Sqrt(e.Value)
//	    return e
//	})
//
//	// Create parallel pipeline with 8 workers, batch size 50
//	parallelPipeline := ParallelPipeline(heavyProcessor, 8, 50)
//
//	result := Collect(parallelPipeline, events)
//
// ## Complex Pipeline Construction
//
//	processData := func(data []*MyEvent) []*ProcessedEvent {
//	    pipeline := Chain(
//	        MapStage(func(e *MyEvent) *MyEvent {
//	            e.Value = math.Log(e.Value + 1)
//	            return e
//	        }),
//	        FilterStage(func(e *MyEvent) bool {
//	            return e.Value > 0.5
//	        }),
//	        ReduceStage(func(e *MyEvent) *ProcessedEvent {
//	            return &ProcessedEvent{
//	                ID:    e.ID,
//	                Score: int(e.Value * 100),
//	            }
//	        }),
//	    )
//
//	    return Collect(pipeline, data)
//	}
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
type Stage[I, O any] func(<-chan I) <-chan O

// MapStage applies a mapping function to each event.
func MapStage[T any](f func(*T) *T) Stage[*T, *T] {
	return func(in <-chan *T) <-chan *T {
		out := make(chan *T, ChannelBufSize)
		go func() {
			defer close(out)
			for v := range in {
				out <- f(v)
			}
		}()
		return out
	}
}

// FilterStage filters events based on a predicate.
func FilterStage[T any](f func(*T) bool) Stage[*T, *T] {
	return func(in <-chan *T) <-chan *T {
		out := make(chan *T, ChannelBufSize)
		go func() {
			defer close(out)
			for v := range in {
				if f(v) {
					out <- v
				}
			}
		}()
		return out
	}
}

// ReduceStage transforms each event from T to R.
func ReduceStage[T, R any](f func(*T) *R) Stage[*T, *R] {
	return func(in <-chan *T) <-chan *R {
		out := make(chan *R, ChannelBufSize)
		go func() {
			defer close(out)
			for v := range in {
				out <- f(v)
			}
		}()
		return out
	}
}

// GenerateStage generates additional events from each input event.
func GenerateStage[T any](f func(*T) []*T) Stage[*T, *T] {
	return func(in <-chan *T) <-chan *T {
		out := make(chan *T, ChannelBufSize)
		go func() {
			defer close(out)
			for v := range in {
				for _, nv := range f(v) {
					out <- nv
				}
			}
		}()
		return out
	}
}

// IfStage routes events to one of two sub-pipelines of the same type based on a condition.
func IfStage[T, U any](cond func(*T) bool, thenStage, elseStage Stage[*T, *U]) Stage[*T, *U] {
	return func(in <-chan *T) <-chan *U {
		out := make(chan *U, ChannelBufSize)
		thenIn := make(chan *T, ChannelBufSize)
		elseIn := make(chan *T, ChannelBufSize)
		thenOut := thenStage(thenIn)
		elseOut := elseStage(elseIn)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for v := range thenOut {
				out <- v
			}
		}()
		go func() {
			defer wg.Done()
			for v := range elseOut {
				out <- v
			}
		}()
		go func() {
			wg.Wait()
			close(out)
		}()
		go func() {
			for v := range in {
				if cond(v) {
					thenIn <- v
				} else {
					elseIn <- v
				}
			}
			close(thenIn)
			close(elseIn)
		}()
		return out
	}
}

// FanOutStage fans out input to multiple workers and merges outputs.
func FanOutStage[I, O any](numWorkers int, worker Stage[I, O]) Stage[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O, numWorkers*ChannelBufSize)
		var ins []chan I
		var outs []<-chan O
		for i := 0; i < numWorkers; i++ {
			inCh := make(chan I, ChannelBufSize)
			ins = append(ins, inCh)
			outs = append(outs, worker(inCh))
		}
		var wg sync.WaitGroup
		for _, ch := range outs {
			wg.Add(1)
			go func(c <-chan O) {
				defer wg.Done()
				for v := range c {
					out <- v
				}
			}(ch)
		}
		go func() {
			wg.Wait()
			close(out)
		}()
		go func() {
			i := 0
			for v := range in {
				ins[i] <- v
				i = (i + 1) % len(ins)
			}
			for _, c := range ins {
				close(c)
			}
		}()
		return out
	}
}

// BatchStage batches input events into slices of size batchSize.
func BatchStage[T any](batchSize int) Stage[*T, []*T] {
	return func(in <-chan *T) <-chan []*T {
		out := make(chan []*T, ChannelBufSize)
		go func() {
			defer close(out)
			batch := make([]*T, 0, batchSize)
			for v := range in {
				batch = append(batch, v)
				if len(batch) == batchSize {
					out <- batch
					batch = make([]*T, 0, batchSize)
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
func FlattenStage[T any]() Stage[[]*T, *T] {
	return func(in <-chan []*T) <-chan *T {
		out := make(chan *T, ChannelBufSize)
		go func() {
			defer close(out)
			for batch := range in {
				for _, v := range batch {
					out <- v
				}
			}
		}()
		return out
	}
}

// ForEachStage applies the processor to each event in a batch.
func ForEachStage[T, R any](processor Stage[*T, *R]) Stage[[]*T, []*R] {
	return func(in <-chan []*T) <-chan []*R {
		out := make(chan []*R, ChannelBufSize)
		go func() {
			defer close(out)
			for batch := range in {
				results := make([]*R, 0, len(batch))
				batchIn := make(chan *T, len(batch))
				batchOut := processor(batchIn)
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for r := range batchOut {
						results = append(results, r)
					}
				}()
				for _, v := range batch {
					batchIn <- v
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
func Chain[I, M, O any](s1 Stage[I, M], s2 Stage[M, O]) Stage[I, O] {
	return func(in <-chan I) <-chan O {
		return s2(s1(in))
	}
}

// ParallelPipeline combines batching, fanning out, and flattening into a processor stage.
func ParallelPipeline[T, R any](processor Stage[*T, *R], numWorkers, batchSize int) Stage[*T, *R] {
	if numWorkers == 0 {
		numWorkers = NumWorkers
	}
	if batchSize == 0 {
		batchSize = BatchSize
	}
	return Chain(
		BatchStage[T](batchSize),
		Chain(
			FanOutStage[[]*T, []*R](
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
		input = nil
	}()
	out := stage(in)
	result := make([]O, 0, len(input))
	for v := range out {
		result = append(result, v)
	}
	return result
}
