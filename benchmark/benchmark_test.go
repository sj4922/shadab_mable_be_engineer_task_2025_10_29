package main

import (
	"fmt"
	"strconv"
	"testing"

	"Mable/pipeline"
)

// testPipeline builds a complex pipeline of all the stages for processing TestStruct elements
func testPipeline() pipeline.Stage[*TestStruct, *TestStruct] {
	mapStage := pipeline.MapStage[TestStruct](func(ts *TestStruct) *TestStruct {
		ts.ID++
		return ts
	})
	filterStage := pipeline.FilterStage[TestStruct](func(ts *TestStruct) bool {
		return ts.Active
	})
	reduceStage := pipeline.ReduceStage[TestStruct, TestStruct](func(ts *TestStruct) *TestStruct {
		ts.Note = "Score: " + strconv.Itoa(ts.Score)
		return ts
	})
	generateStage := pipeline.GenerateStage[TestStruct](func(ts *TestStruct) []*TestStruct {
		if ts.ID%2 == 0 {
			return []*TestStruct{ts, ts}
		}
		return []*TestStruct{ts}
	})
	ifStage := pipeline.IfStage[TestStruct, TestStruct](
		func(ts *TestStruct) bool { return ts.Value > 50 },
		pipeline.MapStage[TestStruct](func(ts *TestStruct) *TestStruct { ts.Value *= 2; return ts }),
		pipeline.MapStage[TestStruct](func(ts *TestStruct) *TestStruct { ts.Value += 10; return ts }),
	)
	processor := pipeline.Chain(
		pipeline.Chain(mapStage, filterStage),
		pipeline.Chain(
			pipeline.Chain(reduceStage, generateStage),
			ifStage,
		),
	)
	return pipeline.ParallelPipeline[TestStruct, TestStruct](processor, pipeline.NumWorkers, pipeline.BatchSize)
}

// mablePipeline builds a complex pipeline of all the stages for processing MableEvent elements
func mablePipeline() pipeline.Stage[*MableEvent, *MableEvent] {
	mapStage := pipeline.MapStage[MableEvent](func(me *MableEvent) *MableEvent {
		me.TS++
		return me
	})
	filterStage := pipeline.FilterStage[MableEvent](func(me *MableEvent) bool {
		return me.EN == "Order Completed"
	})
	reduceStage := pipeline.ReduceStage[MableEvent, MableEvent](func(me *MableEvent) *MableEvent {
		me.PD.PHCT++
		return me
	})
	generateStage := pipeline.GenerateStage[MableEvent](func(me *MableEvent) []*MableEvent {
		if me.TS%2 == 0 {
			return []*MableEvent{me, me}
		}
		return []*MableEvent{me}
	})
	ifStage := pipeline.IfStage[MableEvent, MableEvent](
		func(me *MableEvent) bool { return me.PD.PP > 50 },
		pipeline.MapStage[MableEvent](func(me *MableEvent) *MableEvent { me.PD.PP *= 2; return me }),
		pipeline.MapStage[MableEvent](func(me *MableEvent) *MableEvent { me.PD.PP += 10; return me }),
	)
	processor := pipeline.Chain(
		pipeline.Chain(mapStage, filterStage),
		pipeline.Chain(
			pipeline.Chain(reduceStage, generateStage),
			ifStage,
		),
	)
	return pipeline.ParallelPipeline[MableEvent, MableEvent](processor, pipeline.NumWorkers, pipeline.BatchSize)
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
	stage := testPipeline()
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
	stage := mablePipeline()
	for i := 0; i < b.N; i++ {
		_ = pipeline.Collect(stage, input)
	}
}

// Utility functions for benchmarks
func generateTestInput(n int) []*TestStruct {
	input := make([]*TestStruct, n)
	for i := 0; i < n; i++ {
		input[i] = &TestStruct{
			ID:     i,
			Name:   fmt.Sprintf("test%d", i),
			Value:  float64(i * 10),
			Active: i%2 == 0,
			Score:  i * 5,
		}
	}
	return input
}

func generateMableInput(n int) []*MableEvent {
	input := make([]*MableEvent, n)
	for i := 0; i < n; i++ {
		input[i] = &MableEvent{
			EID: fmt.Sprintf("0197a599-ef42-7bb8-99e7-70e57cef4627-%d", i),
			EN:  "Order Completed",
			TS:  int64(i * 1000),
			PD:  PageData{PP: float64(i) * 10},
		}
	}
	return input
}
