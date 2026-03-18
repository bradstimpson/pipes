package processors

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bradstimpson/pipes/data"
)

func TestNewSQLWriter(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()
	writer := NewSQLWriter(db, "test_table")
	if writer.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", writer.TableName)
	}
}

func TestSQLWriter_ProcessData(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	// Expect Prepare + Exec for INSERT
	mock.ExpectPrepare(`INSERT INTO test_table\(A\) VALUES\(\$1\) ON CONFLICT \(A\) DO UPDATE SET A=EXCLUDED.A`).
		ExpectExec().
		WithArgs(1.0).
		WillReturnResult(sqlmock.NewResult(0, 1))

	writer := NewSQLWriter(db, "test_table")
	outputChan := make(chan data.JSON, 1)
	killChan := make(chan error, 1)
	writer.ProcessData([]byte(`{"A":1}`), outputChan, killChan)
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

// SQLReaderWriter performs both the job of a SQLReader and SQLWriter.
// This means it will run a SQL query, write the resulting data into a
// SQL database, and (if the write was successful) send the queried data
// to the next stage of processing.
//
// SQLReaderWriter is composed of both a SQLReader and SQLWriter, so it
// supports all of the same properties and usage options (such as static
// versus dynamic SQL querying).
type SQLReaderWriter struct {
	SQLReader
	SQLWriter
	ConcurrencyLevel int // See ConcurrentDataProcessor
}

// NewSQLReaderWriter returns a new SQLReaderWriter ready for static querying.
func NewSQLReaderWriter(readConn *sql.DB, writeConn *sql.DB, readQuery, writeTable string) *SQLReaderWriter {
	s := SQLReaderWriter{}
	s.SQLReader = *NewSQLReader(readConn, readQuery)
	s.SQLWriter = *NewSQLWriter(writeConn, writeTable)
	return &s
}

// NewDynamicSQLReaderWriter returns a new SQLReaderWriter ready for dynamic querying.
func NewDynamicSQLReaderWriter(readConn *sql.DB, writeConn *sql.DB, sqlGenerator func(data.JSON) (string, error), writeTable string) *SQLReaderWriter {
	s := NewSQLReaderWriter(readConn, writeConn, "", writeTable)
	s.sqlGenerator = sqlGenerator
	return s
}

// ProcessData uses SQLReader methods for processing data - this works via composition
func (s *SQLReaderWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	s.ForEachQueryData(d, killChan, func(d data.JSON) {
		s.SQLWriter.ProcessData(d, outputChan, killChan)
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (s *SQLReaderWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *SQLReaderWriter) String() string {
	return "SQLReaderWriter"
}

// Concurrency defers to ConcurrentDataProcessor
func (s *SQLReaderWriter) Concurrency() int {
	return s.ConcurrencyLevel
}
