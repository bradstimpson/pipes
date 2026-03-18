package pipes

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/bradstimpson/pipes/data"
	"github.com/bradstimpson/pipes/processors"
	"github.com/bradstimpson/pipes/util"
)

var testDB *sql.DB
var pgContainer *postgres.PostgresContainer

var s3Endpoint string

func TestMain(m *testing.M) {
	var err error

	ctx := context.Background()
	// Patch: Add WaitStrategy and timeout for localstack
	localstackReq := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:latest",
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES": "s3",
		},
		WaitingFor: wait.ForHTTP("_localstack/health").WithStartupTimeout(60 * time.Second),
	}
	s3Container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: localstackReq,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start localstack container: %v\n", err)
		// Print container logs for troubleshooting
		if s3Container != nil {
			logs, logErr := s3Container.Logs(ctx)
			if logErr == nil {
				fmt.Fprintln(os.Stderr, "Localstack logs:")
				io.Copy(os.Stderr, logs)
			}
		}
		os.Exit(1)
	}
	endpoint, err := s3Container.Endpoint(ctx, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get S3 endpoint: %v\n", err)
		os.Exit(1)
	}
	s3Endpoint = endpoint

	pgContainer, err = postgres.Run(ctx,
		"postgres:15",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start postgres container: %v\n", err)
		os.Exit(1)
	}
	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get connection string: %v\n", err)
		os.Exit(1)
	}
	testDB, err = sql.Open("postgres", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to postgres: %v\n", err)
		os.Exit(1)
	}
	for i := 0; i < 10; i++ {
		err = testDB.Ping()
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to postgres after waiting: %v\n", err)
		os.Exit(1)
	}
	_, err = testDB.Exec(`CREATE TABLE IF NOT EXISTS test_table (A INT PRIMARY KEY)`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create test table: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	testDB.Close()
	pgContainer.Terminate(ctx)
	s3Container.Terminate(ctx)
	os.Exit(code)
}

// Benchmark DataProcessor throughput
func BenchmarkDataProcessor(b *testing.B) {
	p := &mockProcessor{}
	dp := Do(p)
	outputChan := make(chan data.JSON, b.N)
	killChan := make(chan error, 1)
	input := []byte("test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dp.ProcessData(input, outputChan, killChan)
	}
}

// Benchmark concurrent processing
func BenchmarkConcurrentProcessing(b *testing.B) {
	p := &mockProcessor{}
	dp := Do(p)
	dp.concurrency = 4
	outputChan := make(chan data.JSON, b.N)
	killChan := make(chan error, 1)
	input := []byte("test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dp.ProcessData(input, outputChan, killChan)
	}
}

// Benchmark SQLWriter
func BenchmarkSQLWriter(b *testing.B) {
	writer := processors.NewSQLWriter(testDB, "test_table")
	outputChan := make(chan data.JSON, b.N)
	go func() {
		for range outputChan {
			// Drain channel
		}
	}()
	killChan := make(chan error, 1)
	input := []byte(`{"A":1}`)
	b.ResetTimer()
	for range b.N {
		writer.ProcessData(input, outputChan, killChan)
	}
}

// Benchmark SQLDumper
func BenchmarkSQLDumper(b *testing.B) {
	dumper := processors.NewSQLDumper(testDB, "test_table")
	dumper.OnDupKeyFields = []string{"A"}
	outputChan := make(chan data.JSON, b.N)
	go func() {
		for range outputChan {
		}
	}()
	killChan := make(chan error, 1)
	input := []byte(`{"A":1}`)
	b.ResetTimer()
	for range b.N {
		dumper.ProcessData(input, outputChan, killChan)
	}
}

// Benchmark CSV processing
func BenchmarkCSVProcess(b *testing.B) {
	params := &util.CSVParameters{
		Writer:        util.NewCSVWriter(),
		WriteHeader:   true,
		HeaderWritten: false,
		SendUpstream:  false,
	}
	params.Writer.SetWriter(&bytes.Buffer{})
	outputChan := make(chan data.JSON, b.N)
	killChan := make(chan error, 1)
	input := []byte(`[{"A":1},{"B":2}]`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		util.CSVProcess(params, input, outputChan, killChan)
	}
}

// Benchmark HTTP processor
func BenchmarkHTTPProcessor(b *testing.B) {
	httpReq, _ := processors.NewHTTPRequest("GET", "http://example.com", nil)
	outputChan := make(chan data.JSON, b.N)
	killChan := make(chan error, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		httpReq.ProcessData([]byte("test"), outputChan, killChan)
	}
}

func stubS3Client() *s3.Client {
	return s3.New(s3.Options{
		Region:           "us-east-1",
		EndpointResolver: s3.EndpointResolverFromURL("http://localhost"),
		HTTPClient:       util.StubHTTPClient(),
	})
}

func BenchmarkS3Writer(b *testing.B) {
	writer := processors.NewS3Writer("test", "test", "us-east-1", "test-bucket", "test-key")
	writer.SetClient(stubS3Client())
	outputChan := make(chan data.JSON, b.N)
	killChan := make(chan error, 1)
	for i := 0; i < b.N; i++ {
		writer.ProcessData([]byte("benchdata"), outputChan, killChan)
	}
	writer.Finish(outputChan, killChan)
}

func BenchmarkS3Reader(b *testing.B) {
	reader := processors.NewS3ObjectReader("test", "test", "us-east-1", "test-bucket", "test-key")
	reader.SetClient(stubS3Client())

	outputChan := make(chan data.JSON, b.N)
	killChan := make(chan error, 1)
	for i := 0; i < b.N; i++ {
		reader.ProcessData([]byte("benchdata"), outputChan, killChan)
	}
}

type mockProcessor struct{}

func (m *mockProcessor) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	outputChan <- d
}
func (m *mockProcessor) Finish(outputChan chan data.JSON, killChan chan error) {}

func (m *mockProcessor) Concurrency() int { return 2 }

// csvToSQLProcessor parses CSV lines into JSON objects.
// The first line received is treated as the header row.
// When lowercase is true, all string values are lowercased.
type csvToSQLProcessor struct {
	header    []string
	lowercase bool
}

func (c *csvToSQLProcessor) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	line := string(d)
	fields := strings.Split(line, ",")
	if c.header == nil {
		c.header = fields
		return
	}
	row := make(map[string]interface{})
	for i, col := range c.header {
		if i < len(fields) {
			v := fields[i]
			if c.lowercase {
				v = strings.ToLower(v)
			}
			row[col] = v
		}
	}
	out, _ := json.Marshal(row)
	outputChan <- data.JSON(out)
}

func (c *csvToSQLProcessor) Finish(outputChan chan data.JSON, killChan chan error) {}

func (c *csvToSQLProcessor) String() string { return "CSVToSQLProcessor" }

func TestE2E_S3_CSV_to_SQL(t *testing.T) {
	ctx := context.Background()

	// Create an S3 client pointing at Localstack
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + s3Endpoint)
		o.UsePathStyle = true
	})

	// Create bucket and upload a CSV file
	bucket := "e2e-csv-bucket"
	key := "data.csv"
	csvData := "id,name,value\n1,Alpha,100\n2,Bravo,200\n3,Charlie,300\n"

	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(csvData),
	})
	if err != nil {
		t.Fatalf("failed to upload CSV to S3: %v", err)
	}

	// Create the SQL table to receive the data
	_, err = testDB.Exec(`CREATE TABLE IF NOT EXISTS e2e_csv (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		value INTEGER NOT NULL
	)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	defer testDB.Exec(`DROP TABLE IF EXISTS e2e_csv`)

	// --- Extract stage: read CSV from S3 line by line ---
	s3Reader := processors.NewS3ObjectReader("test", "test", "us-east-1", bucket, key)
	s3Reader.SetClient(s3Client)

	// --- Transform stage: parse CSV line → JSON, and lowercase all text ---
	csvProc := &csvToSQLProcessor{lowercase: true}

	// --- Load stage: insert JSON objects into Postgres ---
	sqlWriter := processors.NewSQLWriter(testDB, "e2e_csv")
	sqlWriter.OnDupKeyUpdate = false

	// Build a PipelineLayout with explicit ETL stages
	layout, err := NewPipelineLayout(
		// Extract
		NewPipelineStage(
			Do(s3Reader).Outputs(csvProc),
		),
		// Transform
		NewPipelineStage(
			Do(csvProc).Outputs(sqlWriter),
		),
		// Load
		NewPipelineStage(
			Do(sqlWriter),
		),
	)
	if err != nil {
		t.Fatalf("failed to create pipeline layout: %v", err)
	}

	pipeline := NewBranchingPipeline(layout)
	pipeline.Name = "E2E_S3_CSV_to_SQL"
	killChan := pipeline.Run()

	select {
	case err := <-killChan:
		if err != nil {
			t.Fatalf("pipeline error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("pipeline timed out")
	}

	// Verify the data was inserted (names should be lowercased)
	rows, err := testDB.Query(`SELECT id, name, value FROM e2e_csv ORDER BY id`)
	if err != nil {
		t.Fatalf("failed to query results: %v", err)
	}
	defer rows.Close()

	type row struct {
		ID    int
		Name  string
		Value int
	}
	expected := []row{
		{1, "alpha", 100},
		{2, "bravo", 200},
		{3, "charlie", 300},
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ID, &r.Name, &r.Value); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(got))
	}
	for i, e := range expected {
		if got[i] != e {
			t.Errorf("row %d: expected %+v, got %+v", i, e, got[i])
		}
	}
}
