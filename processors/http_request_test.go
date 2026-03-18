package processors

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bradstimpson/pipes"
	"github.com/bradstimpson/pipes/data"
	"github.com/bradstimpson/pipes/logger"
)

func ExampleNewHTTPRequest() {
	logger.LogLevel = logger.LevelSilent

	getGoogle, err := NewHTTPRequest("GET", "http://www.google.com", nil)
	if err != nil {
		panic(err)
	}
	// this is just a really basic checking function so we can have
	// determinable example output.
	checkHTML := NewFuncTransformer(func(d data.JSON) data.JSON {
		output := "Got HTML?\n"
		if strings.Contains(strings.ToLower(string(d)), "html") {
			output += "YES\n"
		} else {
			output += "NO\n"
		}
		output += "HTML contains Google Search?\n"
		if strings.Contains(string(d), "Google Search") {
			output += "YES\n"
		} else {
			output += "NO\n"
		}
		return data.JSON(output)
	})
	stdout := NewIoWriter(os.Stdout)
	pipeline := pipes.NewPipeline(getGoogle, checkHTML, stdout)

	err = <-pipeline.Run()

	if err != nil {
		fmt.Println("An error occurred in the pipes pipeline:", err.Error())
	}

	// Output:
	// Got HTML?
	// YES
	// HTML contains Google Search?
	// YES
}

func TestNewHTTPRequest(t *testing.T) {
	httpReq, err := NewHTTPRequest("GET", "http://example.com", nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if httpReq.Request.Method != "GET" {
		t.Errorf("Expected method 'GET', got '%s'", httpReq.Request.Method)
	}
	if httpReq.Request.URL.String() != "http://example.com" {
		t.Errorf("Expected URL 'http://example.com', got '%s'", httpReq.Request.URL.String())
	}
}
