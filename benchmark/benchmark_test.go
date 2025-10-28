package main

import (
	"testing"

	"Mable/pipeline"
)

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
	stage := TestPipeline()
	for i := 0; i < b.N; i++ {
		_ = pipeline.Collect(stage, input)
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
	stage := MablePipeline()
	for i := 0; i < b.N; i++ {
		_ = pipeline.Collect(stage, input)
	}
}
