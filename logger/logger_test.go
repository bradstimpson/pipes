package logger

import (
	"testing"
)

func TestLogLevel(t *testing.T) {
	LevelSilent := LevelSilent
	LevelDebug := LevelDebug
	LevelInfo := LevelInfo
	LevelError := LevelError
	if LevelDebug >= LevelSilent {
		t.Errorf("LevelDebug should be less than LevelSilent")
	}
	if LevelDebug >= LevelInfo {
		t.Errorf("LevelDebug should be less than LevelInfo")
	}
	if LevelInfo >= LevelError {
		t.Errorf("LevelInfo should be less than LevelError")
	}
}

func TestLogFunctions(t *testing.T) {
	// These should not panic
	Debug("debug message")
	Info("info message")
	Error("error message")
}
