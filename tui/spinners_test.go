package tui

import (
	"testing"
)

func TestSpinnerTheme(t *testing.T) {
	th := Theme{
		Saucer:        "=",
		SaucerHead:    ">",
		SaucerPadding: " ",
		BarStart:      "[",
		BarEnd:        "]",
	}
	if th.Saucer != "=" {
		t.Errorf("Expected Saucer to be '='")
	}
}
