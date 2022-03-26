package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/inc"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/reader"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/util"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/writer"
	"github.com/spf13/viper"
)

var fullUpdateFlag = flag.Bool("f", false, "Perform full update even if incremental update is available")
var incUpdateFlag = flag.Bool("i", true, "Incremental update [deprecated]")
var datadirFlag = flag.String("D", "", "LDP data directory")
var ldpUserFlag = flag.String("u", "", "LDP user to be granted select privileges")
var noTrigramIndexFlag = flag.Bool("T", false, "Disable creation of trigram indexes")
var verboseFlag = flag.Bool("v", false, "Enable verbose output")
var csvFilenameFlag = flag.String("c", "", "Write output to CSV file instead of a database")
var srsRecordsFlag = flag.String("r", "public.srs_records", "Name of table containing SRS records to read")
var srsMarcFlag = flag.String("m", "public.srs_marc", "Name of table containing SRS MARC (JSON) data to read")
var srsMarcAttrFlag = flag.String("j", "data", "Name of column containing MARC JSON data")
var helpFlag = flag.Bool("h", false, "Help for ldpmarc")

var tableoutSchema = "ldpmarc"
var tableoutTable = "_srs_marctab"
var tableout = tableoutSchema + "." + tableoutTable
var tablefinalSchema = "public"
var tablefinalTable = "srs_marctab"
var tablefinal = tablefinalSchema + "." + tablefinalTable

var allFields = util.GetAllFieldNames()

var csvFile *os.File

var program = "ldpmarc"

func main() {
	flag.Parse()
	if len(flag.Args()) > 0 {
		printerr("invalid argument: %s", flag.Arg(0))
		os.Exit(2)
	}
	if *helpFlag || *datadirFlag == "" {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", program)
		flag.PrintDefaults()
		if *helpFlag {
			return
		} else {
			os.Exit(2)
		}
	}
	var err error
	if err = run(); err != nil {
		printerr("%s", err)
		os.Exit(1)
	}
}

func run() error {
	// Read database configuration
	var ldpconf = filepath.Join(*datadirFlag, "ldpconf.json")
	viper.SetConfigFile(ldpconf)
	viper.SetConfigType("json")
	var err error
	var ok bool
	if err = viper.ReadInConfig(); err != nil {
		if _, ok = err.(viper.ConfigFileNotFoundError); ok {
			return fmt.Errorf("file not found: %s", ldpconf)
		} else {
			return fmt.Errorf("error reading file: %s: %s", ldpconf, err)
		}
	}
	var ldp = "ldp_database"
	var host = viper.GetString(ldp + ".database_host")
	var port = strconv.Itoa(viper.GetInt(ldp + ".database_port"))
	var user = viper.GetString(ldp + ".database_user")
	var password = viper.GetString(ldp + ".database_password")
	var dbname = viper.GetString(ldp + ".database_name")
	var sslmode = viper.GetString(ldp + ".database_sslmode")
	// Open database
	var db *sql.DB
	if db, err = openDB(host, port, user, password, dbname, sslmode); err != nil {
		return err
	}
	var incUpdateAvail bool
	if incUpdateAvail, err = inc.IncUpdateAvail(db); err != nil {
		return err
	}
	_ = incUpdateFlag
	if incUpdateAvail && !*fullUpdateFlag && *csvFilenameFlag == "" {
		printerr("incremental update")
		if err = inc.IncUpdate(db, *srsRecordsFlag, *srsMarcFlag, *srsMarcAttrFlag, tablefinal, printerr, *verboseFlag); err != nil {
			return err
		}
	} else {
		printerr("full update")
		// Catch SIGTERM etc.
		c := make(chan os.Signal, 2)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			for _ = range c {
				_, _ = fmt.Fprintf(os.Stderr, "\nldpmarc: canceling due to user request\n")
				_, _ = fmt.Fprintf(os.Stderr, "ldpmarc: cleaning up temporary files\n")
				_, _ = db.ExecContext(context.TODO(), "DROP TABLE IF EXISTS "+tableout)
				for _, field := range allFields {
					_, _ = db.ExecContext(context.TODO(), "DROP TABLE IF EXISTS "+tableout+"_"+field)
				}
				os.Exit(1)
			}
		}()
		// Run full update
		if err = fullUpdate(db); err != nil {
			_, _ = db.ExecContext(context.TODO(), "DROP TABLE IF EXISTS "+tableout)
			return err
		}
	}
	return nil
}

func openDB(host, port, user, password, dbname, sslmode string) (*sql.DB, error) {
	var connstr = "host=" + host + " port=" + port + " user=" + user + " password=" + password + " dbname=" + dbname + " sslmode=" + sslmode
	var err error
	var db *sql.DB
	if db, err = sql.Open("postgres", connstr); err != nil {
		return nil, fmt.Errorf("unable to open database: %s: %s", dbname, err)
	}
	return db, nil
}

func fullUpdate(db *sql.DB) error {
	var err error
	if *csvFilenameFlag != "" {
		if csvFile, err = os.Create(*csvFilenameFlag); err != nil {
			return err
		}
		defer func(csvFile *os.File) {
			_ = csvFile.Close()
		}(csvFile)
		printerr("output will be written to file: %s", *csvFilenameFlag)
	}
	// Process MARC data
	inputCount, err := process(db)
	if err != nil {
		return err
	}
	if *csvFilenameFlag == "" {
		// Index columns
		if err = index(db); err != nil {
			return err
		}
		// Replace table
		if err = replace(db); err != nil {
			return err
		}
		// Grant permission to LDP user
		if ldpUserFlag != nil && *ldpUserFlag != "" {
			if err = grant(db, *ldpUserFlag); err != nil {
				return err
			}
		}
		_, _ = db.ExecContext(context.TODO(), "DROP TABLE IF EXISTS dbsystem.ldpmarc_cksum;")
		_, _ = db.ExecContext(context.TODO(), "DROP TABLE IF EXISTS dbsystem.ldpmarc_metadata;")
		if inputCount > 0 {
			printerr("writing checksums")
			if err = inc.CreateCksum(db, *srsRecordsFlag, *srsMarcFlag, *srsMarcAttrFlag); err != nil {
				return err
			}
		}
		printerr("vacuuming")
		if err = util.VacuumAnalyze(db, tablefinal); err != nil {
			return err
		}
		if err = inc.VacuumCksum(db); err != nil {
			return err
		}
		printerr("new table is ready to use: " + tablefinal)
	}
	return nil
}

func process(db *sql.DB) (int64, error) {
	var err error
	if db != nil {
		if err = setupTable(db); err != nil {
			return 0, err
		}
	}

	// Begin reader transaction
	var txin *sql.Tx
	if txin, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		return 0, err
	} // Deferred txin.Rollback() causes process to hang
	// read number of input records
	var inputCount int64
	if inputCount, err = selectCount(txin, *srsRecordsFlag); err != nil {
		return 0, err
	}
	printerr("transforming %d input records", inputCount)
	// main processing
	var rch <-chan reader.Record = reader.ReadAll(txin, *srsRecordsFlag, *srsMarcFlag, *srsMarcAttrFlag)
	var writeCount int64
	if inputCount > 0 {
		if writeCount, err = processAll(db, rch); err != nil {
			return 0, err
		}
	}
	// Commit
	if err = txin.Rollback(); err != nil {
		return 0, err
	}
	printerr("%d output rows", writeCount)
	return inputCount, nil
}

func setupTable(db *sql.DB) error {
	var err error
	var q string
	q = "CREATE SCHEMA IF NOT EXISTS " + tableoutSchema + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema: %s", err)
	}
	q = "CREATE SCHEMA IF NOT EXISTS " + tableoutSchema + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema: %s", err)
	}
	_, _ = db.ExecContext(context.TODO(), "DROP TABLE IF EXISTS "+tableout)
	var lz4 string
	if util.IsLZ4Available(db) {
		lz4 = " COMPRESSION lz4"
	}
	q = "" +
		"CREATE TABLE " + tableout + " (" +
		"    srs_id varchar(36) NOT NULL," +
		"    line smallint NOT NULL," +
		"    matched_id varchar(36) NOT NULL," +
		"    instance_hrid varchar(32) NOT NULL," +
		"    instance_id varchar(36) NOT NULL," +
		"    field varchar(3) NOT NULL," +
		"    ind1 varchar(1) NOT NULL," +
		"    ind2 varchar(1) NOT NULL," +
		"    ord smallint NOT NULL," +
		"    sf varchar(1) NOT NULL," +
		"    content varchar(65535)" + lz4 + " NOT NULL" +
		") PARTITION BY LIST (field);"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table: %s", err)
	}
	for _, field := range allFields {
		_, _ = db.ExecContext(context.TODO(), "DROP TABLE IF EXISTS "+tableout+"_"+field)
		q = "CREATE TABLE " + tableout + "_" + field +
			" PARTITION OF " + tableout + " FOR VALUES IN ('" + field + "');"
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("creating partition: %s", err)
		}
	}
	return nil
}

func selectCount(txin *sql.Tx, tablein string) (int64, error) {
	var err error
	var count int64
	var q = "SELECT count(*) FROM " + tablein + ";"
	if err = txin.QueryRowContext(context.TODO(), q).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func processAll(db *sql.DB, rch <-chan reader.Record) (int64, error) {
	var err error
	// Begin output transaction
	var txout *sql.Tx
	if txout, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		return 0, err
	}
	var writeCount int64
	var wch chan writer.Record
	var wclosed <-chan error
	if txout != nil {
		wch, wclosed = writer.WriteAll(txout, tableoutSchema, tableoutTable)
	}
	for {
		var inrec reader.Record = <-rch
		if inrec.Stop {
			if inrec.Err != nil {
				return 0, err
			}
			break
		}
		var id, matchedID, instanceHRID, instanceID string
		var mrecs []srs.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(inrec, printerr, *verboseFlag)
		if skip {
			continue
		}
		var m srs.Marc
		for _, m = range mrecs {
			if txout != nil {
				wch <- writer.Record{
					Close:        false,
					Stop:         false,
					Err:          nil,
					SRSID:        id,
					Line:         m.Line,
					MatchedID:    matchedID,
					InstanceHRID: instanceHRID,
					InstanceID:   instanceID,
					Field:        m.Field,
					Ind1:         m.Ind1,
					Ind2:         m.Ind2,
					Ord:          m.Ord,
					SF:           m.SF,
					Content:      m.Content,
				}
			} else {
				_, _ = fmt.Fprintf(csvFile, "%q,%d,%q,%q,%q,%q,%q,%q,%d,%q,%q\n", id, m.Line, matchedID, instanceHRID, instanceID, m.Field, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content)
			}
			writeCount++
		}
	}

	if txout != nil {
		wch <- writer.Record{Close: true}
		err = <-wclosed
		if err != nil {
			return 0, err
		}
	}

	if err = txout.Commit(); err != nil {
		return 0, err
	}

	return writeCount, nil
}

func index(db *sql.DB) error {
	var err error
	// Index columns
	var cols = []string{"content", "matched_id", "instance_hrid", "instance_id", "ind1", "ind2", "ord", "sf"}
	if err = indexColumns(db, cols); err != nil {
		return err
	}
	// Create unique index
	printerr("creating index: srs_id, line, field")
	var q = "CREATE UNIQUE INDEX ON " + tableout + " (srs_id, line, field);"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating index: srs_id, line, field: %s", err)
	}
	return nil
}

func indexColumns(db *sql.DB, cols []string) error {
	var err error
	var c string
	for _, c = range cols {
		if c == "content" {
			if !*noTrigramIndexFlag {
				printerr("creating index: %s", c)
				var q = "CREATE INDEX ON " + tableout + " USING GIN (" + c + " gin_trgm_ops);"
				if _, err = db.ExecContext(context.TODO(), q); err != nil {
					return fmt.Errorf("creating index with pg_trgm extension: %s: %s", c, err)
				}
			}
		} else {
			printerr("creating index: %s", c)
			var q = "CREATE INDEX ON " + tableout + " (" + c + ");"
			if _, err = db.ExecContext(context.TODO(), q); err != nil {
				return fmt.Errorf("creating index: %s: %s", c, err)
			}
		}
	}
	return nil
}

func replace(db *sql.DB) error {
	var err error
	var q = "DROP TABLE IF EXISTS folio_source_record.__marc;"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + tablefinalTable + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableout + " RENAME TO " + tablefinalTable + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("renaming table: %s", err)
	}
	q = "DROP TABLE IF EXISTS " + tablefinal + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableoutSchema + "." + tablefinalTable + " SET SCHEMA " + tablefinalSchema + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("moving table: %s", err)
	}
	for _, field := range allFields {
		q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + tablefinalTable + "_" + field + ";"
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("dropping table: %s", err)
		}
		q = "ALTER TABLE " + tableout + "_" + field + " RENAME TO " + tablefinalTable + "_" + field + ";"
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("renaming table: %s", err)
		}
	}
	return nil
}

func grant(db *sql.DB, user string) error {
	var err error
	// Grant permission to LDP user
	var q = "GRANT USAGE ON SCHEMA " + tablefinalSchema + " TO " + user + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("schema permission: %s", err)
	}
	q = "GRANT SELECT ON " + tablefinal + " TO " + user + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("table permission: %s", err)
	}
	return nil
}

func printerr(format string, v ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", program, fmt.Sprintf(format, v...))
}
