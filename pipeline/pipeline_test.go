package pipeline

import (
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"
)

type TestStruct struct {
	ID      int
	Name    string
	Value   float64
	Active  bool
	Tags    []string
	Data    map[string]int
	Created time.Time
	Updated time.Time
	Score   int
	Note    string
}

// generateInput creates a slice of TestStruct with n elements for testing
func generateInput(n int) []TestStruct {
	input := make([]TestStruct, n)
	for i := 0; i < n; i++ {
		input[i] = TestStruct{
			ID:      i,
			Name:    "test",
			Value:   float64(i),
			Active:  i%2 == 0,
			Tags:    []string{"tag1", "tag2"},
			Data:    map[string]int{"key": i},
			Created: time.Now(),
			Updated: time.Now(),
			Score:   i * 10,
			Note:    "note",
		}
	}
	return input
}

// getDynamicConfig computes optimal number of workers and batch size based on input size
func getDynamicConfig(inputSize int) (numWorkers, batchSize int) {
	numWorkers = runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}

	defaultBatchSize := 1000
	if inputSize > 0 {
		batchSize = inputSize / (numWorkers * 4)
		if batchSize < 100 {
			batchSize = 100
		} else if batchSize > 10000 {
			batchSize = 10000
		}
	} else {
		batchSize = defaultBatchSize * numWorkers
		if batchSize > 10000 {
			batchSize = 10000
		}
	}
	return numWorkers, batchSize
}

// fullPipeline builds a complex pipeline of stages for processing TestStruct elements
func fullPipeline(inputSize int) Stage[TestStruct, TestStruct] {
	numWorkers, batchSize := getDynamicConfig(inputSize)

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
	thenStage := MapStage[TestStruct](func(ts TestStruct) TestStruct {
		ts.Value *= 2
		return ts
	})
	elseStage := MapStage[TestStruct](func(ts TestStruct) TestStruct {
		ts.Value += 10
		return ts
	})
	ifStage := IfStage[TestStruct, TestStruct](func(ts TestStruct) bool {
		return ts.Value > 50
	}, thenStage, elseStage)
	return MetricStage("pipeline", Chain(
		BatchStage[TestStruct](batchSize),
		Chain(
			FanOutStage[[]TestStruct, []TestStruct](
				numWorkers,
				ForEachStage[TestStruct, TestStruct](
					Chain(
						Chain(mapStage, filterStage),
						Chain(
							Chain(reduceStage, generateStage),
							ifStage,
						),
					),
				),
			),
			FlattenStage[TestStruct](),
		),
	))
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

// Tests DefaultMetricsConfig with environment variables
func TestDefaultMetricsConfig(t *testing.T) {
	t.Setenv("CLICKHOUSE_HOST", "test-host")
	t.Setenv("CLICKHOUSE_PORT", "1234")
	t.Setenv("CLICKHOUSE_DB", "test-db")
	t.Setenv("CLICKHOUSE_USER", "test-user")
	t.Setenv("CLICKHOUSE_PASSWORD", "test-pass")
	config := DefaultMetricsConfig()
	expected := MetricsConfig{
		Host:     "test-host",
		Port:     1234,
		Database: "test-db",
		Username: "test-user",
		Password: "test-pass",
	}
	if !reflect.DeepEqual(config, expected) {
		t.Errorf("expected %v, got %v", expected, config)
	}
}

// Tests DefaultMetricsConfig with default values
func TestDefaultMetricsConfigDefaults(t *testing.T) {
	t.Setenv("CLICKHOUSE_HOST", "")
	t.Setenv("CLICKHOUSE_PORT", "")
	t.Setenv("CLICKHOUSE_DB", "")
	t.Setenv("CLICKHOUSE_USER", "")
	t.Setenv("CLICKHOUSE_PASSWORD", "")
	config := DefaultMetricsConfig()
	expected := MetricsConfig{
		Host:     "localhost",
		Port:     9000,
		Database: "default",
		Username: "default",
		Password: "default",
	}
	if !reflect.DeepEqual(config, expected) {
		t.Errorf("expected %v, got %v", expected, config)
	}
}

// BenchmarkFullPipeline10 benchmarks the full pipeline with 10 events
func BenchmarkFullPipeline10(b *testing.B) {
	benchmarkFullPipeline(b, 10)
}

// BenchmarkFullPipeline100 benchmarks the full pipeline with 100 events
func BenchmarkFullPipeline100(b *testing.B) {
	benchmarkFullPipeline(b, 100)
}

// BenchmarkFullPipeline1000 benchmarks the full pipeline with 10,000 events
func BenchmarkFullPipeline1000(b *testing.B) {
	benchmarkFullPipeline(b, 10000)
}

// BenchmarkFullPipeline100000 benchmarks the full pipeline with 100,000 events
func BenchmarkFullPipeline100000(b *testing.B) {
	benchmarkFullPipeline(b, 100000)
}

// BenchmarkFullPipeline1000000 benchmarks the full pipeline with 1,000,000 events
func BenchmarkFullPipeline1000000(b *testing.B) {
	benchmarkFullPipeline(b, 1000000)
}

// BenchmarkFullPipeline10000000 benchmarks the full pipeline with 10,000,000 events
func BenchmarkFullPipeline10000000(b *testing.B) {
	benchmarkFullPipeline(b, 10000000)
}

// benchmarkFullPipeline runs the full pipeline n times and logs performance metrics
func benchmarkFullPipeline(b *testing.B, n int) {
	input := generateInput(n)
	stage := fullPipeline(n)
	b.ReportAllocs()
	b.ResetTimer()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	startAllocs := memStats.Mallocs
	startBytes := memStats.TotalAlloc

	for i := 0; i < b.N; i++ {
		_ = Collect[TestStruct, TestStruct](stage, input)
	}

	b.StopTimer()
	runtime.ReadMemStats(&memStats)
	logMetric(
		b.Name(),
		b.Elapsed()/time.Duration(b.N),
		n,
		b.Elapsed().Nanoseconds()/int64(b.N),
		int64(memStats.TotalAlloc-startBytes)/int64(b.N),
		int64(memStats.Mallocs-startAllocs)/int64(b.N),
	)
}
