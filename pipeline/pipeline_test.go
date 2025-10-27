package pipeline

import (
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

// testPipeline builds a complex pipeline of all the stages for processing TestStruct elements
func testPipeline() Stage[TestStruct, TestStruct] {
	mapStage := MapStage[TestStruct](func(ts TestStruct) TestStruct {
		ts.ID++
		return ts
	})
	filterStage := FilterStage[TestStruct](func(ts TestStruct) bool {
		return ts.Active
	})
	reduceStage := ReduceStage[TestStruct, TestStruct](func(ts TestStruct) TestStruct {
		ts.Note = "Score: " + strconv.Itoa(ts.Score)
		return ts
	})
	generateStage := GenerateStage[TestStruct](func(ts TestStruct) []TestStruct {
		if ts.ID%2 == 0 {
			return []TestStruct{ts, ts}
		}
		return []TestStruct{ts}
	})
	ifStage := IfStage[TestStruct, TestStruct](
		func(ts TestStruct) bool { return ts.Value > 50 },
		MapStage[TestStruct](func(ts TestStruct) TestStruct { ts.Value *= 2; return ts }),
		MapStage[TestStruct](func(ts TestStruct) TestStruct { ts.Value += 10; return ts }),
	)
	processor := Chain(
		Chain(mapStage, filterStage),
		Chain(
			Chain(reduceStage, generateStage),
			ifStage,
		),
	)
	return MetricStage("test_pipeline",
		ParallelPipeline[TestStruct, TestStruct](processor, NumWorkers, BatchSize),
	)
}

// mablePipeline builds a complex pipeline of all the stages for processing MableEvent elements
func mablePipeline() Stage[MableEvent, MableEvent] {
	mapStage := MapStage[MableEvent](func(me MableEvent) MableEvent {
		me.TS++
		return me
	})
	filterStage := FilterStage[MableEvent](func(me MableEvent) bool {
		return me.EN == "Order Completed"
	})
	reduceStage := ReduceStage[MableEvent, MableEvent](func(me MableEvent) MableEvent {
		me.PD.PHCT++
		return me
	})
	generateStage := GenerateStage[MableEvent](func(me MableEvent) []MableEvent {
		if me.TS%2 == 0 {
			return []MableEvent{me, me}
		}
		return []MableEvent{me}
	})
	ifStage := IfStage[MableEvent, MableEvent](
		func(me MableEvent) bool { return me.PD.PP > 50 },
		MapStage[MableEvent](func(me MableEvent) MableEvent { me.PD.PP *= 2; return me }),
		MapStage[MableEvent](func(me MableEvent) MableEvent { me.PD.PP += 10; return me }),
	)
	processor := Chain(
		Chain(mapStage, filterStage),
		Chain(
			Chain(reduceStage, generateStage),
			ifStage,
		),
	)
	return MetricStage("mable_pipeline",
		ParallelPipeline[MableEvent, MableEvent](processor, NumWorkers, BatchSize),
	)
}

// Tests MapStage with integer multiplication
func TestMapStage(t *testing.T) {
	input := []int{1, 2, 3}
	output := Collect(MapStage[int](func(x int) int { return x * 2 }), input)
	if !reflect.DeepEqual(output, []int{2, 4, 6}) {
		t.Errorf("expected [2 4 6], got %v", output)
	}
}

// Tests FilterStage with even number filtering
func TestFilterStage(t *testing.T) {
	input := []int{1, 2, 3, 4}
	output := Collect(FilterStage[int](func(x int) bool { return x%2 == 0 }), input)
	if !reflect.DeepEqual(output, []int{2, 4}) {
		t.Errorf("expected [2 4], got %v", output)
	}
}

// Tests ReduceStage with value conversion
func TestReduceStage(t *testing.T) {
	input := []int{1, 2, 3}
	output := Collect(ReduceStage[int, string](func(x int) string { return string('a' + rune(x-1)) }), input)
	if !reflect.DeepEqual(output, []string{"a", "b", "c"}) {
		t.Errorf("expected [a b c], got %v", output)
	}
}

// Tests GenerateStage with slice expansion
func TestGenerateStage(t *testing.T) {
	input := []int{1, 2}
	output := Collect(GenerateStage[int](func(x int) []int { return []int{x, x + 1} }), input)
	if !reflect.DeepEqual(output, []int{1, 2, 2, 3}) {
		t.Errorf("expected [1 2 2 3], got %v", output)
	}
}

// Tests IfStage with conditional mapping
func TestIfStage(t *testing.T) {
	input := []int{1, 2, 3, 4}
	stage := IfStage[int, int](
		func(x int) bool { return x%2 == 0 },
		MapStage[int](func(x int) int { return x * 2 }),
		MapStage[int](func(x int) int { return x + 1 }),
	)
	output := Collect(stage, input)
	expected := []int{2, 4, 4, 8}
	sort.Ints(output)
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("expected %v, got %v", expected, output)
	}
}

// Tests FanOutStage with parallel processing
func TestFanOutStage(t *testing.T) {
	input := []int{1, 2, 3, 4}
	output := Collect(FanOutStage[int, int](2, MapStage[int](func(x int) int { return x * 2 })), input)
	expected := []int{2, 4, 6, 8}
	sort.Ints(output)
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("expected %v, got %v", expected, output)
	}
}

// Tests BatchStage with batching logic
func TestBatchStage(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	output := Collect(BatchStage[int](3), input)
	if !reflect.DeepEqual(output, [][]int{{1, 2, 3}, {4, 5}}) {
		t.Errorf("expected [[1 2 3] [4 5]], got %v", output)
	}
}

// Tests FlattenStage with slice flattening
func TestFlattenStage(t *testing.T) {
	input := [][]int{{1, 2}, {3, 4, 5}}
	output := Collect(FlattenStage[int](), input)
	if !reflect.DeepEqual(output, []int{1, 2, 3, 4, 5}) {
		t.Errorf("expected [1 2 3 4 5], got %v", output)
	}
}

// Tests ForEachStage with slice mapping
func TestForEachStage(t *testing.T) {
	input := [][]int{{1, 2}, {3, 4}}
	output := Collect(ForEachStage[int, int](MapStage[int](func(x int) int { return x * 2 })), input)
	if !reflect.DeepEqual(output, [][]int{{2, 4}, {6, 8}}) {
		t.Errorf("expected [[2 4] [6 8]], got %v", output)
	}
}

// Tests ParallelPipeline with concurrent execution
func TestParallelPipeline(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6}
	output := Collect(ParallelPipeline[int, int](MapStage[int](func(x int) int { return x * 2 }), 2, 3), input)
	expected := []int{2, 4, 6, 8, 10, 12}
	sort.Ints(output)
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("expected %v, got %v", expected, output)
	}
}

// Tests Collect function
func TestCollect(t *testing.T) {
	input := []string{"a", "b", "c"}
	output := Collect(MapStage[string](func(s string) string { return s + "!" }), input)
	if !reflect.DeepEqual(output, []string{"a!", "b!", "c!"}) {
		t.Errorf("expected [a! b! c!], got %v", output)
	}
}

// Tests Chain with stage composition
func TestChain(t *testing.T) {
	input := []int{1, 2, 3}
	stage := Chain(
		MapStage[int](func(x int) int { return x + 1 }),
		FilterStage[int](func(x int) bool { return x > 2 }),
	)
	output := Collect(stage, input)
	if !reflect.DeepEqual(output, []int{3, 4}) {
		t.Errorf("expected [3 4], got %v", output)
	}
}

// Tests MetricStage with instrumentation
func TestMetricStage(t *testing.T) {
	input := []int{1, 2}
	output := Collect(MetricStage[int, int]("test", MapStage[int](func(x int) int { return x })), input)
	if !reflect.DeepEqual(output, []int{1, 2}) {
		t.Errorf("expected [1 2], got %v", output)
	}
}

// Tests MapStage with empty input
func TestMapStageEmptyInput(t *testing.T) {
	output := Collect(MapStage[int](func(x int) int { return x * 2 }), []int{})
	if !reflect.DeepEqual(output, []int{}) {
		t.Errorf("expected [], got %v", output)
	}
}

// Tests FilterStage with empty input
func TestFilterStageEmptyInput(t *testing.T) {
	output := Collect(FilterStage[int](func(x int) bool { return x%2 == 0 }), []int{})
	if !reflect.DeepEqual(output, []int{}) {
		t.Errorf("expected [], got %v", output)
	}
}

// Tests GenerateStage with empty input
func TestGenerateStageEmptyInput(t *testing.T) {
	output := Collect(GenerateStage[int](func(x int) []int { return []int{x, x + 1} }), []int{})
	if !reflect.DeepEqual(output, []int{}) {
		t.Errorf("expected [], got %v", output)
	}
}

// Tests IfStage with empty input
func TestIfStageEmptyInput(t *testing.T) {
	stage := IfStage[int, int](
		func(x int) bool { return x%2 == 0 },
		MapStage[int](func(x int) int { return x * 2 }),
		MapStage[int](func(x int) int { return x + 1 }),
	)
	output := Collect(stage, []int{})
	if !reflect.DeepEqual(output, []int{}) {
		t.Errorf("expected [], got %v", output)
	}
}

// Tests FanOutStage with single worker
func TestFanOutStageSingleWorker(t *testing.T) {
	input := []int{1, 2, 3}
	output := Collect(FanOutStage[int, int](1, MapStage[int](func(x int) int { return x * 2 })), input)
	if !reflect.DeepEqual(output, []int{2, 4, 6}) {
		t.Errorf("expected [2 4 6], got %v", output)
	}
}

// Tests BatchStage with empty input
func TestBatchStageEmptyInput(t *testing.T) {
	output := Collect(BatchStage[int](3), []int{})
	if !reflect.DeepEqual(output, [][]int{}) {
		t.Errorf("expected [], got %v", output)
	}
}

// Tests FlattenStage with empty input
func TestFlattenStageEmptyInput(t *testing.T) {
	output := Collect(FlattenStage[int](), [][]int{})
	if !reflect.DeepEqual(output, []int{}) {
		t.Errorf("expected [], got %v", output)
	}
}

// Tests generateTestInput
func TestGenerateTestInput(t *testing.T) {
	input := generateTestInput(2)
	if len(input) != 2 {
		t.Errorf("expected length 2, got %d", len(input))
	}
	if input[0].ID != 0 || input[1].ID != 1 {
		t.Errorf("expected IDs 0,1, got %d,%d", input[0].ID, input[1].ID)
	}
}

// Tests generateMableInput
func TestGenerateMableInput(t *testing.T) {
	input := generateMableInput(2)
	if len(input) != 2 {
		t.Errorf("expected length 2, got %d", len(input))
	}
	if input[0].EID != "0197a599-ef42-7bb8-99e7-70e57cef4627-0" || input[1].EID != "0197a599-ef42-7bb8-99e7-70e57cef4627-1" {
		t.Errorf("expected specific EIDs, got %s, %s", input[0].EID, input[1].EID)
	}
}

// Tests loadMableEventSample
func TestLoadMableEventSample(t *testing.T) {
	event, err := loadMableEventSample()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if event.EID != "0197a599-ef42-7bb8-99e7-70e57cef4627" {
		t.Errorf("expected specific EID, got %s", event.EID)
	}
}

// Tests ParseMableEventJSON with valid JSON
func TestParseMableEventJSON(t *testing.T) {
	data := []byte(`{"eid":"test","en":"test_event","ts":123}`)
	event, err := ParseMableEventJSON(data)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if event.EID != "test" {
		t.Errorf("expected EID test, got %s", event.EID)
	}
}

// Tests ParseMableEventJSON with invalid JSON
func TestParseMableEventJSONInvalid(t *testing.T) {
	data := []byte(`invalid json`)
	_, err := ParseMableEventJSON(data)
	if err == nil {
		t.Errorf("expected error, got nil")

	}
}

// Tests MustGetEnvAsInt with valid env
func TestMustGetEnvAsInt(t *testing.T) {
	os.Setenv("TEST_INT", "42")
	defer os.Unsetenv("TEST_INT")
	val := getEnvAsIntOnly("TEST_INT", 10)
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

// Tests MustGetEnvAsInt with invalid env value
func TestMustGetEnvAsIntInvalid(t *testing.T) {
	os.Setenv("TEST_INT_INVALID", "notanumber")
	defer os.Unsetenv("TEST_INT_INVALID")
	val := getEnvAsIntOnly("TEST_INT_INVALID", 10)
	if val != 10 {
		t.Errorf("expected 10, got %d", val)
	}
}

// Tests MustGetEnvAsInt when not set
func TestMustGetEnvAsIntNotSet(t *testing.T) {
	val := getEnvAsIntOnly("NOT_SET", 10)
	if val != 10 {
		t.Errorf("expected 10, got %d", val)
	}
}

// Tests getEnvOrDefault when set
func TestGetEnvOrDefaultSet(t *testing.T) {
	os.Setenv("TEST_STR", "value")
	defer os.Unsetenv("TEST_STR")
	val := getEnvOrDefault("TEST_STR", "default")
	if val != "value" {
		t.Errorf("expected value, got %s", val)
	}
}

// Tests getEnvOrDefault when not set
func TestGetEnvOrDefaultNotSet(t *testing.T) {
	val := getEnvOrDefault("NOT_SET", "default")
	if val != "default" {
		t.Errorf("expected default, got %s", val)
	}
}

// BenchmarkTestPipeline10 benchmarks the test pipeline with 10 events
func BenchmarkTestPipeline10(b *testing.B) {
	benchmarkTestPipeline(b, 10)
}

// BenchmarkTestPipeline100 benchmarks the test pipeline with 100 events
func BenchmarkTestPipeline100(b *testing.B) {
	benchmarkTestPipeline(b, 100)
}

// BenchmarkTestPipeline10000 benchmarks the test pipeline with 10,000 events
func BenchmarkTestPipeline10000(b *testing.B) {
	benchmarkTestPipeline(b, 10000)
}

// BenchmarkTestPipeline100000 benchmarks the test pipeline with 100,000 events
func BenchmarkTestPipeline100000(b *testing.B) {
	benchmarkTestPipeline(b, 100000)
}

// BenchmarkTestPipeline1000000 benchmarks the test pipeline with 1,000,000 events
func BenchmarkTestPipeline1000000(b *testing.B) {
	benchmarkTestPipeline(b, 1000000)
}

// BenchmarkTestPipeline10000000 benchmarks the test pipeline with 10,000,000 events
func BenchmarkTestPipeline10000000(b *testing.B) {
	benchmarkTestPipeline(b, 10000000)
}

// benchmarkTestPipeline runs the test pipeline n times and logs performance metrics
func benchmarkTestPipeline(b *testing.B, n int) {
	input := generateTestInput(n)
	stage := testPipeline()
	for i := 0; i < b.N; i++ {
		_ = Collect[TestStruct, TestStruct](stage, input)
	}
}

// BenchmarkMablePipeline10 benchmarks the optimized Mable pipeline with 10 events
func BenchmarkMablePipeline10(b *testing.B) {
	benchmarkMablePipeline(b, 10)
}

// BenchmarkMablePipeline100 benchmarks the optimized Mable pipeline with 100 events
func BenchmarkMablePipeline100(b *testing.B) {
	benchmarkMablePipeline(b, 100)
}

// BenchmarkMablePipeline10000 benchmarks the optimized Mable pipeline with 10,000 events
func BenchmarkMablePipeline10000(b *testing.B) {
	benchmarkMablePipeline(b, 10000)
}

// BenchmarkMablePipeline100000 benchmarks the optimized Mable pipeline with 100,000 events
func BenchmarkMablePipeline100000(b *testing.B) {
	benchmarkMablePipeline(b, 100000)
}

// BenchmarkMablePipeline1000000 benchmarks the optimized Mable pipeline with 1,000,000 events
func BenchmarkMablePipeline1000000(b *testing.B) {
	benchmarkMablePipeline(b, 1000000)
}

// BenchmarkMablePipeline10000000 benchmarks the optimized Mable pipeline with 10,000,000 events
func BenchmarkMablePipeline10000000(b *testing.B) {
	benchmarkMablePipeline(b, 10000000)
}

// benchmarkMablePipeline runs the optimized Mable pipeline n times and logs performance metrics
func benchmarkMablePipeline(b *testing.B, n int) {
	input := generateMableInput(n)
	stage := mablePipeline()
	for i := 0; i < b.N; i++ {
		_ = Collect[MableEvent, MableEvent](stage, input)
	}
}
