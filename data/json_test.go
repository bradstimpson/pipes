package data

import (
	"testing"
)

func TestObjectsFromJSON_Null(t *testing.T) {
	input := []byte("null")
	objects, err := ObjectsFromJSON(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("Expected empty slice, got %v", objects)
	}
}

func TestObjectsFromJSON_Array(t *testing.T) {
	input := []byte(`[{"A":1},{"B":2}]`)
	objects, err := ObjectsFromJSON(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(objects) != 2 {
		t.Errorf("Expected 2 objects, got %d", len(objects))
	}
}

func TestObjectsFromJSON_Object(t *testing.T) {
	input := []byte(`{"A":1,"B":2}`)
	objects, err := ObjectsFromJSON(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(objects) != 1 {
		t.Errorf("Expected 1 object, got %d", len(objects))
	}
}
