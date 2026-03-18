package processors

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/bradstimpson/pipes/data"
	"github.com/bradstimpson/pipes/logger"
	"github.com/bradstimpson/pipes/tui"
	"github.com/bradstimpson/pipes/util"
	"github.com/k0kubun/go-ansi"
)

// sql_dump handles large data dumps into the postgres database.

type SQLDumper struct {
	writeDB          *sql.DB
	TableName        string
	OnDupKeyUpdate   bool
	OnDupKeyFields   []string
	ConcurrencyLevel int // See ConcurrentDataProcessor
	BatchSize        int
	ProgressBar      bool
}

type SQLDumperData struct {
	TableName  string `json:"table_name"`
	InsertData any    `json:"insert_data"`
}

// NewSQLDumper returns a new SQLDumper
func NewSQLDumper(db *sql.DB, tableName string) *SQLDumper {
	logger.Info("SQLDumper: Initializing SQLDumper with db: ", db, "and tableName:", tableName)
	return &SQLDumper{writeDB: db, TableName: tableName, OnDupKeyUpdate: true}
}

// ProcessData defers to util.SQLInsertData
func (s *SQLDumper) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	defer func() {
		if err := recover(); err != nil {
			util.KillPipelineIfErr(err.(error), killChan)
		}
	}()
	logger.Info("SQLDumper: Writing data...", string(d))
	dumped := SQLDumpData(s.writeDB, d, s.TableName, s.OnDupKeyUpdate, s.OnDupKeyFields, s.BatchSize, s.ProgressBar)
	util.KillPipelineIfErr(dumped, killChan)
	logger.Info("SQLDumper: Write complete")
}

// Finish - see interface for documentation.
func (s *SQLDumper) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *SQLDumper) String() string {
	return "SQLDumper"
}

// Concurrency defers to ConcurrentDataProcessor
func (s *SQLDumper) Concurrency() int {
	return s.ConcurrencyLevel
}

func SQLDumpData(db *sql.DB, d data.JSON, tableName string, onDupKeyUpdate bool, onDupKeyFields []string, batchSize int, progressBar bool) error {
	objects, err := data.ObjectsFromJSON(d)
	if err != nil {
		return err
	}

	if batchSize > 0 {
		for i := 0; i < len(objects); i += batchSize {
			maxIndex := min(i+batchSize, len(objects))
			err = dumpSql(true, db, objects[i:maxIndex], tableName, onDupKeyUpdate, onDupKeyFields, progressBar)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return dumpSql(true, db, objects, tableName, onDupKeyUpdate, onDupKeyFields, progressBar)
}

func dumpSql(dump bool, db *sql.DB, objects []map[string]interface{}, tableName string, onDupKeyUpdate bool, onDupKeyFields []string, progressBar bool) error {
	var bar *tui.ProgressBar
	cols := sortedColumns(objects)
	if dump {
		logger.Debug("SQLDumpData: dump mode: ", dump)
		if progressBar {
			bar = tui.NewOptions(len(objects),
				tui.OptionSetWriter(ansi.NewAnsiStdout()),
				tui.OptionEnableColorCodes(true),
				tui.OptionShowBytes(true),
				tui.OptionSetWidth(15),
				tui.OptionSetDescription("[cyan][1/3][reset] Pushing to database..."),
				tui.OptionSetTheme(tui.Theme{
					Saucer:        "[green]=[reset]",
					SaucerHead:    "[green]>[reset]",
					SaucerPadding: " ",
					BarStart:      "[",
					BarEnd:        "]",
				}))
		}
		for i := range objects {
			if progressBar {
				bar.Add(1)
			}
			var insertSQL = fmt.Sprintf("INSERT INTO %v(%v) VALUES", tableName, strings.Join(cols, ","))
			var qs strings.Builder
			qs.WriteString("(")
			obj := objects[i]
			var check = 0
			for _, o := range obj {
				switch o := o.(type) {
				case bool:
					if o {
						qs.WriteString("'true'")
					} else {
						qs.WriteString("'false'")
					}
				case float64:
					fl := fmt.Sprintf("%v", o)
					qs.WriteString(fl)
				default:
					qs.WriteString("'" + o.(string) + "'")
					logger.Debug("SQLtypeCheck: No valid type detected.")
				}
				check++
				if check != len(obj) {
					qs.WriteString(",")
				} else {
					break
				}
			}
			qs.WriteString(")")
			insertSQL += qs.String()
			// Add ON CONFLICT upsert logic if enabled
			if onDupKeyUpdate && len(onDupKeyFields) > 0 {
				conflictTarget := onDupKeyFields[0]
				insertSQL += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET ", conflictTarget)
				for j, c := range onDupKeyFields {
					if j > 0 {
						insertSQL += ", "
					}
					insertSQL += fmt.Sprintf("%s=EXCLUDED.%s", c, c)
				}
			}
			logger.Debug("SQLDumpData:", insertSQL)
			_, err := db.Exec(insertSQL)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func sortedColumns(objects []map[string]any) []string {
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
