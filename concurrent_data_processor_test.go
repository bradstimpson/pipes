package pipes

import (
	"testing"

	"github.com/bradstimpson/pipes/data"
)

func TestIsConcurrent(t *testing.T) {
	var p DataProcessor = &mockProcessor{}
	if !isConcurrent(p) {
		t.Errorf("Expected processor to be concurrent")
	}
}

func TestConcurrentDataProcessor_ProcessData(t *testing.T) {
	mock := &mockProcessor{}
	dp := Do(mock)
	outputChan := make(chan data.JSON, 2)
	killChan := make(chan error, 1)
	go dp.ProcessData([]byte("test"), outputChan, killChan)
	result := <-outputChan
	if string(result) != "test" {
		t.Errorf("Expected 'test', got '%s'", string(result))
	}
}
