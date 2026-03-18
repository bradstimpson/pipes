package pipes

import (
	"testing"
	"github.com/bradstimpson/pipes/data"
)

type simpleProcessor struct{}

func (s *simpleProcessor) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	outputChan <- d
}
func (s *simpleProcessor) Finish(outputChan chan data.JSON, killChan chan error) {}

func TestDoWrapsProcessor(t *testing.T) {
	sp := &simpleProcessor{}
	dp := Do(sp)
	if dp.DataProcessor != sp {
		t.Errorf("Do should wrap the processor instance")
	}
}

func TestOutputsSetsProcessors(t *testing.T) {
	sp := &simpleProcessor{}
	dp := Do(sp)
	other := &simpleProcessor{}
	dp.Outputs(other)
	if len(dp.outputs) != 1 {
		t.Errorf("Expected 1 output processor, got %d", len(dp.outputs))
	}
}

func TestBranchOutCopiesData(t *testing.T) {
	sp := &simpleProcessor{}
	dp := Do(sp)
	dp.branchOutChans = []chan data.JSON{make(chan data.JSON, 1)}
	dp.outputChan = make(chan data.JSON, 1)
	dp.outputChan <- []byte("test")
	close(dp.outputChan)
	dp.branchOut()
	result := <-dp.branchOutChans[0]
	if string(result) != "test" {
		t.Errorf("Expected 'test', got '%s'", string(result))
	}
}
