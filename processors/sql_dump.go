package processors

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/bradstimpson/pipes/data"
	"github.com/bradstimpson/pipes/logger"
	"github.com/bradstimpson/pipes/util"
	"github.com/gary23w/goapp/internal/utils"
	"github.com/k0kubun/go-ansi"
)

// sql_dump handles large data dumps into the postgres database.

type SQLWriter struct {
	writeDB          *sql.DB
	TableName        string
	OnDupKeyUpdate   bool
	OnDupKeyFields   []string
	ConcurrencyLevel int // See ConcurrentDataProcessor
	BatchSize        int
}

type SQLWriterData struct {
	TableName  string      `json:"table_name"`
	InsertData interface{} `json:"insert_data"`
}

// NewSQLWriter returns a new SQLWriter
func NewSQLWriter(db *sql.DB, tableName string) *SQLWriter {
	return &SQLWriter{writeDB: db, TableName: tableName, OnDupKeyUpdate: true}
}

// ProcessData defers to util.SQLInsertData
func (s *SQLWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	defer func() {
		if err := recover(); err != nil {
			util.KillPipelineIfErr(err.(error), killChan)
		}
	}()

	logger.Info("SQLWriterv2: Writing data...")
	test := SQLInsertData(s.writeDB, d, s.TableName, s.OnDupKeyUpdate, s.OnDupKeyFields, s.BatchSize)
	util.KillPipelineIfErr(test, killChan)
	logger.Info("SQLWriter: Write complete")
}

// Finish - see interface for documentation.
func (s *SQLWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *SQLWriter) String() string {
	return "SQLWriter"
}

// Concurrency defers to ConcurrentDataProcessor
func (s *SQLWriter) Concurrency() int {
	return s.ConcurrencyLevel
}

func SQLInsertData(db *sql.DB, d data.JSON, tableName string, onDupKeyUpdate bool, onDupKeyFields []string, batchSize int) error {
	objects, err := data.ObjectsFromJSON(d)
	fmt.Println(objects)
	if err != nil {
		return err
	}

	if batchSize > 0 {
		for i := 0; i < len(objects); i += batchSize {
			maxIndex := i + batchSize
			if maxIndex > len(objects) {
				maxIndex = len(objects)
			}
			err = dumpSql(true, db, objects[i:maxIndex], tableName, onDupKeyUpdate, onDupKeyFields)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return dumpSql(true, db, objects, tableName, onDupKeyUpdate, onDupKeyFields)
}

func dumpSql(dump bool, db *sql.DB, objects []map[string]interface{}, tableName string, onDupKeyUpdate bool, onDupKeyFields []string) error {
	cols := sortedColumns(objects)
	fmt.Println(cols)
	if dump {
		logger.Debug("SQLDumpData: dump mode: %v", dump)
		bar := utils.NewOptions(len(objects),
			utils.OptionSetWriter(ansi.NewAnsiStdout()),
			utils.OptionEnableColorCodes(true),
			utils.OptionShowBytes(true),
			utils.OptionSetWidth(15),
			utils.OptionSetDescription("[cyan][1/3][reset] Pushing to database..."),
			utils.OptionSetTheme(utils.Theme{
				Saucer:        "[green]=[reset]",
				SaucerHead:    "[green]>[reset]",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}))

		for i := 0; i < len(objects); i++ {
			bar.Add(1)
			var insertSQL = fmt.Sprintf("INSERT INTO %v(%v) VALUES", tableName, strings.Join(cols, ","))
			qs := "("
			obj := objects[i]
			var check = 0
			for _, o := range obj {
				//type check
				switch o.(type) {
				case bool:
					if o.(bool) {
						qs += "'true'"
					} else {
						qs += "'false'"
					}
				case float64:
					fl := fmt.Sprintf("%v", o.(float64))
					qs += fl
				default:
					qs += "'" + o.(string) + "'"
					logger.Debug("SQLtypeCheck: No valid type detected.")
				}

				logger.Debug("SQLtableDump: New table DUMP")
				check++
				if check != len(obj) {
					qs += ","
				} else {
					break
				}
			}
			qs += ")"
			insertSQL += qs
			logger.Debug("SQLDumpData:", insertSQL)
			_, err := db.Exec(insertSQL)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func sortedColumns(objects []map[string]interface{}) []string {
	// Collect data keys
	colsMap := make(map[string]struct{})
	for _, o := range objects {
		for col := range o {
			colsMap[col] = struct{}{}
		}
	}
	cols := []string{}
	for col := range colsMap {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}
