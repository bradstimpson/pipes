package pipes_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/bradstimpson/pipes"
	"github.com/bradstimpson/pipes/data"
	"github.com/bradstimpson/pipes/logger"
	"github.com/bradstimpson/pipes/processors"
)

func ExampleNewPipeline() {
	logger.LogLevel = logger.LevelSilent

	// A basic pipeline is created using one or more DataProcessor instances.
	hello := processors.NewIoReader(strings.NewReader("Hello world!"))
	stdout := processors.NewIoWriter(os.Stdout)
	pipeline := pipes.NewPipeline(hello, stdout)

	err := <-pipeline.Run()

	if err != nil {
		fmt.Println("An error occurred in the pipes pipeline:", err.Error())
	}

	// Output:
	// Hello world!
}

func ExampleNewBranchingPipeline() {
	logger.LogLevel = logger.LevelSilent

	// This example is very contrived, but we'll first create
	// DataProcessors that will spit out strings, do some basic
	// transformation, and then filter out all the ones that don't
	// match "HELLO".
	hello := processors.NewIoReader(strings.NewReader("Hello world"))
	hola := processors.NewIoReader(strings.NewReader("Hola mundo"))
	bonjour := processors.NewIoReader(strings.NewReader("Bonjour monde"))
	upperCaser := processors.NewFuncTransformer(func(d data.JSON) data.JSON {
		return data.JSON(strings.ToUpper(string(d)))
	})
	lowerCaser := processors.NewFuncTransformer(func(d data.JSON) data.JSON {
		return data.JSON(strings.ToLower(string(d)))
	})
	helloMatcher := processors.NewRegexpMatcher("HELLO")
	stdout := processors.NewIoWriter(os.Stdout)

	// Create the PipelineLayout that will run the DataProcessors
	layout, err := pipes.NewPipelineLayout(
		// Stage 1 - spits out hello world in a few languages
		pipes.NewPipelineStage(
			pipes.Do(hello).Outputs(upperCaser, lowerCaser),
			pipes.Do(hola).Outputs(upperCaser),
			pipes.Do(bonjour).Outputs(lowerCaser),
		),
		// Stage 2 - transforms strings to upper and lower case
		pipes.NewPipelineStage(
			pipes.Do(upperCaser).Outputs(helloMatcher),
			pipes.Do(lowerCaser).Outputs(helloMatcher),
		),
		// Stage 3 - only lets through strings that match "hello"
		pipes.NewPipelineStage(
			pipes.Do(helloMatcher).Outputs(stdout),
		),
		// Stage 4 - prints to STDOUT
		pipes.NewPipelineStage(
			pipes.Do(stdout),
		),
	)
	if err != nil {
		panic(err.Error())
	}

	// Create and run the Pipeline
	pipeline := pipes.NewBranchingPipeline(layout)
	err = <-pipeline.Run()

	if err != nil {
		fmt.Println("An error occurred in the pipes pipeline:", err.Error())
	}

	// Output:
	// HELLO WORLD
}
