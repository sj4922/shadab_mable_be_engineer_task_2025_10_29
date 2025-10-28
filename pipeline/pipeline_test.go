package pipeline

import (
	"reflect"
	"sort"
	"testing"
)

// Tests MapStage with integer multiplication
func TestMapStage(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2), ptrOf(3)}
	stage := MapStage[int](func(x *int) *int {
		val := *x * 2
		return &val
	})
	output := collectInts(Collect[*int, *int](stage, input))
	expected := []int{2, 4, 6}
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("expected %v, got %v", expected, output)
	}
}

// Tests FilterStage with even number filtering
func TestFilterStage(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2), ptrOf(3), ptrOf(4)}
	stage := FilterStage[int](func(x *int) bool {
		return *x%2 == 0
	})
	output := collectInts(Collect[*int, *int](stage, input))
	if !reflect.DeepEqual(output, []int{2, 4}) {
		t.Errorf("expected [2 4], got %v", output)
	}
}

// Tests ReduceStage with value conversion
func TestReduceStage(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2), ptrOf(3)}
	stage := ReduceStage[int, string](func(x *int) *string {
		val := string('a' + rune(*x-1))
		return &val
	})
	output := collectStrings(Collect[*int, *string](stage, input))
	if !reflect.DeepEqual(output, []string{"a", "b", "c"}) {
		t.Errorf("expected [a b c], got %v", output)
	}
}

// Tests GenerateStage with slice expansion
func TestGenerateStage(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2)}
	output := Collect[*int, *int](GenerateStage[int](func(x *int) []*int {
		v1 := *x
		v2 := *x + 1
		return []*int{&v1, &v2}
	}), input)
	outputInts := collectInts(output)
	sort.Ints(outputInts)
	if !reflect.DeepEqual(outputInts, []int{1, 2, 2, 3}) {
		t.Errorf("expected %v, got %v", []int{1, 2, 2, 3}, outputInts)
	}
}

// Tests IfStage with conditional mapping
func TestIfStage(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2), ptrOf(3), ptrOf(4)}
	stage := IfStage[int, int](
		func(x *int) bool { return (*x)%2 == 0 },
		MapStage[int](func(x *int) *int { val := *x * 2; return &val }),
		MapStage[int](func(x *int) *int { val := *x + 1; return &val }),
	)
	output := collectInts(Collect[*int, *int](stage, input))
	expected := []int{2, 4, 4, 8}
	sort.Ints(output)
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("expected %v, got %v", expected, output)
	}
}

// Tests FanOutStage with parallel processing
func TestFanOutStage(t *testing.T) {
	worker := func(in <-chan int) <-chan int {
		out := make(chan int, 10)
		go func() {
			defer close(out)
			for v := range in {
				out <- v * 2
			}
		}()
		return out
	}
	input := []int{1, 2, 3, 4}
	output := Collect[int, int](FanOutStage[int, int](2, worker), input)
	expected := []int{2, 4, 6, 8}
	sort.Ints(output)
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("expected %v, got %v", expected, output)
	}
}

// Tests BatchStage with batching logic
func TestBatchStage(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2), ptrOf(3), ptrOf(4), ptrOf(5)}
	output := Collect[*int, []*int](BatchStage[int](3), input)
	outputInts := collectIntSlices(output)
	expected := [][]int{{1, 2, 3}, {4, 5}}
	if !reflect.DeepEqual(outputInts, expected) {
		t.Errorf("expected %v, got %v", expected, outputInts)
	}
}

// Tests FlattenStage with slice flattening
func TestFlattenStage(t *testing.T) {
	input := [][]*int{{ptrOf(1), ptrOf(2)}, {ptrOf(3), ptrOf(4), ptrOf(5)}}
	output := collectInts(Collect[[]*int, *int](FlattenStage[int](), input))
	if !reflect.DeepEqual(output, []int{1, 2, 3, 4, 5}) {
		t.Errorf("expected [1 2 3 4 5], got %v", output)
	}
}

// Tests ForEachStage with slice mapping
func TestForEachStage(t *testing.T) {
	input := [][]*int{{ptrOf(1), ptrOf(2)}, {ptrOf(3), ptrOf(4)}}
	output := Collect[[]*int, []*int](ForEachStage[int, int](MapStage[int](func(x *int) *int { val := *x * 2; return &val })), input)
	outputInts := collectIntSlices(output)
	expected := [][]int{{2, 4}, {6, 8}}
	if !reflect.DeepEqual(outputInts, expected) {
		t.Errorf("expected [[2 4] [6 8]], got %v", outputInts)
	}
}

// Tests ParallelPipeline with concurrent execution
func TestParallelPipeline(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2), ptrOf(3), ptrOf(4), ptrOf(5), ptrOf(6)}
	output := Collect[*int, *int](ParallelPipeline[int, int](MapStage[int](func(x *int) *int { val := *x * 2; return &val }), 2, 3), input)
	outputInts := collectInts(output)
	expected := []int{2, 4, 6, 8, 10, 12}
	sort.Ints(outputInts)
	if !reflect.DeepEqual(outputInts, expected) {
		t.Errorf("expected %v, got %v", expected, outputInts)
	}
}

// Tests Collect function
func TestCollect(t *testing.T) {
	input := []*string{ptrOf("a"), ptrOf("b"), ptrOf("c")}
	output := collectStrings(Collect[*string, *string](MapStage[string](func(s *string) *string { val := *s + "!"; return &val }), input))
	if !reflect.DeepEqual(output, []string{"a!", "b!", "c!"}) {
		t.Errorf("expected [a! b! c!], got %v", output)
	}
}

// Tests Chain with stage composition
func TestChain(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2), ptrOf(3)}
	stage := Chain[*int, *int, *int](
		MapStage[int](func(x *int) *int { val := *x + 1; return &val }),
		FilterStage[int](func(x *int) bool { return *x > 2 }),
	)
	output := collectInts(Collect[*int, *int](stage, input))
	if !reflect.DeepEqual(output, []int{3, 4}) {
		t.Errorf("expected [3 4], got %v", output)
	}
}

// Empty input tests
func TestMapStageEmptyInput(t *testing.T) {
	output := collectInts(Collect[*int, *int](MapStage[int](func(x *int) *int { val := *x * 2; return &val }), []*int{}))
	if len(output) != 0 {
		t.Errorf("expected empty slice, got %v", output)
	}
}

// Tests FilterStage with empty input
func TestFilterStageEmptyInput(t *testing.T) {
	output := collectInts(Collect[*int, *int](FilterStage[int](func(x *int) bool { return *x%2 == 0 }), []*int{}))
	if len(output) != 0 {
		t.Errorf("expected empty slice, got %v", output)
	}
}

// Tests GenerateStage with empty input
func TestGenerateStageEmptyInput(t *testing.T) {
	output := collectInts(Collect[*int, *int](GenerateStage[int](func(x *int) []*int { val := *x; return []*int{&val, &val} }), []*int{}))
	if len(output) != 0 {
		t.Errorf("expected empty slice, got %v", output)
	}
}

// Tests IfStage with empty input
func TestIfStageEmptyInput(t *testing.T) {
	stage := IfStage[int, int](
		func(x *int) bool { return *x%2 == 0 },
		MapStage[int](func(x *int) *int { val := *x * 2; return &val }),
		MapStage[int](func(x *int) *int { val := *x + 1; return &val }),
	)
	output := collectInts(Collect[*int, *int](stage, []*int{}))
	if len(output) != 0 {
		t.Errorf("expected empty slice, got %v", output)
	}
}

// Tests FanOutStage with single worker
func TestFanOutStageSingleWorker(t *testing.T) {
	input := []int{1, 2, 3}
	output := Collect[int, int](FanOutStage[int, int](1, func(in <-chan int) <-chan int {
		out := make(chan int, 10)
		go func() {
			defer close(out)
			for v := range in {
				out <- v * 2
			}
		}()
		return out
	}), input)
	sort.Ints(output)
	if !reflect.DeepEqual(output, []int{2, 4, 6}) {
		t.Errorf("expected [2 4 6], got %v", output)
	}
}

// Tests BatchStage with empty input
func TestBatchStageEmptyInput(t *testing.T) {
	output := Collect[*int, []*int](BatchStage[int](3), []*int{})
	outputInts := collectIntSlices(output)
	if len(outputInts) != 0 {
		t.Errorf("expected empty slice, got %v", outputInts)
	}
}

func TestFlattenStageEmptyInput(t *testing.T) {
	output := collectInts(Collect[[]*int, *int](FlattenStage[int](), [][]*int{}))
	if len(output) != 0 {
		t.Errorf("expected empty slice, got %v", output)
	}
}

// Tests MetricStage with instrumentation
func TestMetricStage(t *testing.T) {
	input := []*int{ptrOf(1), ptrOf(2)}
	output := collectInts(Collect[*int, *int](MetricStage[*int, *int]("test", MapStage[int](func(x *int) *int { return x })), input))
	if !reflect.DeepEqual(output, []int{1, 2}) {
		t.Errorf("expected [1 2], got %v", output)
	}
}

// Tests LogMetric function directly
func TestLogMetric(t *testing.T) {
	LogMetric("test_stage", 10, 1.5, 1024)
}

// Helper to collect int pointers to []int
func collectInts(output []*int) []int {
	var ints []int
	for _, p := range output {
		ints = append(ints, *p)
	}
	return ints
}

// Helper to collect string pointers to []string
func collectStrings(output []*string) []string {
	var strs []string
	for _, p := range output {
		strs = append(strs, *p)
	}
	return strs
}

// Helper to collect int slice pointers to [][]int
func collectIntSlices(output [][]*int) [][]int {
	var slices [][]int
	for _, slice := range output {
		var ints []int
		for _, p := range slice {
			ints = append(ints, *p)
		}
		slices = append(slices, ints)
	}
	return slices
}
