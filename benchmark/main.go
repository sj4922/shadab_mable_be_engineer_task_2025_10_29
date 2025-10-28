// Package main is a pipeline processing tool.
//
// It runs a pipeline on input JSON data from --input file, saving processed output to --output file.
// Metrics are logged via MetricStage to ClickHouse.
//
// Usage: ./benchmark [options]
// Options:
//
//	-input string       Input JSON file path
//	-output string      Output JSON file path
//	-pipeline string    Pipeline type: TestPipeline or MablePipeline
//
// Docker: Set PIPELINE_* env vars to override default hyperparameters.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

	"Mable/pipeline"
)

func main() {
	var inputFile, outputFile, pipelineType string

	flag.StringVar(&inputFile, "input", "", "Input JSON file path")
	flag.StringVar(&outputFile, "output", "", "Output JSON file path")
	flag.StringVar(&pipelineType, "pipeline", "", "Pipeline type: TestPipeline or MablePipeline")

	flag.Parse()

	if flag.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "Unexpected arguments: %v\n", flag.Args())
		os.Exit(1)
	}

	if inputFile == "" {
		fmt.Fprintf(os.Stderr, "Error: --input is required\n")
		os.Exit(1)
	}
	if outputFile == "" {
		fmt.Fprintf(os.Stderr, "Error: --output is required\n")
		os.Exit(1)
	}
	if pipelineType == "" {
		fmt.Fprintf(os.Stderr, "Error: --pipeline is required\n")
		os.Exit(1)
	}

	switch pipelineType {
	case "TestPipeline":
		runBenchmark[*TestStruct](inputFile, func() pipeline.Stage[*TestStruct, *TestStruct] {
			return TestPipeline()
		}, outputFile)
	case "MablePipeline":
		runBenchmark[*MableEvent](inputFile, func() pipeline.Stage[*MableEvent, *MableEvent] {
			return MablePipeline()
		}, outputFile)
	default:
		fmt.Fprintf(os.Stderr, "Invalid pipeline type: %s. Use TestPipeline or MablePipeline.\n", pipelineType)
		os.Exit(1)
	}
}

func runBenchmark[T any](inputFile string, pipelineFunc func() pipeline.Stage[T, T], outputFile string) {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}

	var input []T
	if err := json.Unmarshal(data, &input); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	stage := pipelineFunc()

	result := pipeline.Collect(stage, input)

	if outputFile != "" {
		outputData, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling output: %v\n", err)
			os.Exit(1)
		}
		err = os.WriteFile(outputFile, outputData, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing output file: %v\n", err)
			os.Exit(1)
		}
	}
	_ = result
}

// TestPipeline builds a complex pipeline of all the stages for processing TestStruct elements
func TestPipeline() pipeline.Stage[*TestStruct, *TestStruct] {
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
	return pipeline.MetricStage("test_pipeline",
		pipeline.ParallelPipeline[TestStruct, TestStruct](processor, pipeline.NumWorkers, pipeline.BatchSize),
	)
}

// MablePipeline builds a complex pipeline of all the stages for processing MableEvent elements
func MablePipeline() pipeline.Stage[*MableEvent, *MableEvent] {
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
	return pipeline.MetricStage("mable_pipeline",
		pipeline.ParallelPipeline[MableEvent, MableEvent](processor, pipeline.NumWorkers, pipeline.BatchSize),
	)
}
