package util

import (
	"testing"
)

func TestCSVString_Nil(t *testing.T) {
	if CSVString(nil) != "" {
		t.Errorf("Expected empty string for nil input")
	}
}

func TestCSVString_Value(t *testing.T) {
	if CSVString(123) != "123" {
		t.Errorf("Expected '123' for input 123")
	}
	if CSVString("abc") != "abc" {
		t.Errorf("Expected 'abc' for input 'abc'")
	}
}
