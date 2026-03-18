package processors

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bradstimpson/pipes/data"
	_ "github.com/lib/pq"
)

func TestNewSQLDumper(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	dumper := NewSQLDumper(db, "test_table")
	if dumper.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", dumper.TableName)
	}
}

func TestSQLDumper_ProcessData(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	dumper := NewSQLDumper(db, "test_table")
	dumper.ProgressBar = false

	// Expect Exec for the actual SQL generated
	mock.ExpectExec("INSERT INTO test_table\\(A\\) VALUES\\(1\\)").WillReturnResult(sqlmock.NewResult(1, 1))

	outputChan := make(chan data.JSON, 1)
	killChan := make(chan error, 1)
	dumper.ProcessData([]byte(`{"A":1}`), outputChan, killChan)
	select {
	case err := <-killChan:
		if err != nil {
			t.Errorf("ProcessData returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		// No error, pass
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}
