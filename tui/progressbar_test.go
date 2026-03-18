package tui

import (
	"testing"
)

func TestNewOptions(t *testing.T) {
	bar := NewOptions(10)
	if bar == nil {
		t.Errorf("Expected progress bar to be initialized")
	}
}
