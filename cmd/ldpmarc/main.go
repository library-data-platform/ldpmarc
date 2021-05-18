package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/reader"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
	"github.com/spf13/viper"
)

var datadirFlag = flag.String("D", "", "LDP data directory")
var ldpUserFlag = flag.String("u", "", "LDP user to be granted select privileges")
var noParallelVacuumFlag = flag.Bool("P", false, "Disable parallel vacuum (PostgreSQL 13 or later only)")
var noTrigramIndexFlag = flag.Bool("T", false, "Disable creation of trigram indexes")
var numberOfRecordsFlag = flag.Int("N", -1, "Number of records to process or -1 for all records")
var verboseFlag = flag.Bool("v", false, "Enable verbose output")
var csvFilenameFlag = flag.String("c", "", "Write output to CSV file instead of a database")
var helpFlag = flag.Bool("h", false, "Help for ldpmarc")

var srsRecords = "public.srs_records"
var srsMarc = "public.srs_marc"
var tableoutSchema = "folio_source_record"
var tableoutTable = "__srs_marc"
var tableout = tableoutSchema + "." + tableoutTable
var tablefinalTable = "__marc"
var tablefinal = tableoutSchema + "." + tablefinalTable

var csvFile *os.File

var program = "ldpmarc"

func main() {
	flag.Parse()
	if len(flag.Args()) > 0 {
		printerr("invalid argument: %s", flag.Arg(0))
		os.Exit(2)
	}
	if *helpFlag || *datadirFlag == "" || (*ldpUserFlag == "" && *csvFilenameFlag == "") {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", program)
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
	printerr("completed")
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
	// Begin output transaction
	var txout *sql.Tx
	if *csvFilenameFlag == "" {
		if txout, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
			return err
		}
		defer txout.Rollback()
	} else {
		if csvFile, err = os.Create(*csvFilenameFlag); err != nil {
			return err
		}
		defer csvFile.Close()
		printerr("output will be written to file: %s", *csvFilenameFlag)
	}
	// Process MARC data
	if err = process(db, txout); err != nil {
		return err
	}
	if *csvFilenameFlag == "" {
		// Index columns
		if err = index(txout); err != nil {
			return err
		}
		// Replace table
		if err = replace(txout); err != nil {
			return err
		}
		// Grant permission to LDP user
		if err = grant(txout, *ldpUserFlag); err != nil {
			return err
		}
		// Commit
		if err = txout.Commit(); err != nil {
			return err
		}
		printerr("new table \"" + tablefinal + "\" is ready to use")
		printerr("vacuuming")
		// Vacuum
		var q = "VACUUM "
		if *noParallelVacuumFlag {
			q = q + "(PARALLEL 0) "
		}
		q = q + tablefinal + ";"
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("vacuuming:  %s", err)
		}
		// Analyze
		q = "ANALYZE " + tablefinal + ";"
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("analyzing:  %s", err)
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

func process(db *sql.DB, txout *sql.Tx) error {
	var err error
	if txout != nil {
		if err = setupTable(txout); err != nil {
			return err
		}
	}

	// Begin reader transaction
	var txin *sql.Tx
	if txin, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		return err
	} // Deferred txin.Rollback() causes process to hang
	// Start reader
	printerr("reading tables: %s %s", srsMarc, srsRecords)
	var r *reader.Reader
	var inputCount int64
	if r, inputCount, err = reader.NewReader(txin, srsRecords, srsMarc, *verboseFlag, *numberOfRecordsFlag); err != nil {
		return err
	} // Deferred r.Close() causes process to hang
	printerr("transforming %d input records", inputCount)
	var writeCount int64
	if writeCount, err = transform(txout, r); err != nil {
		return err
	}
	r.Close()
	// Commit
	if err = txin.Rollback(); err != nil {
		return err
	}
	printerr("%d output rows", writeCount)
	return nil
}

func setupTable(txout *sql.Tx) error {
	var err error
	var q = "CREATE SCHEMA IF NOT EXISTS " + tableoutSchema + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating schema: %s", err)
	}
	q = "" +
		"CREATE TABLE " + tableout + " (" +
		"    srs_id varchar(36) NOT NULL," +
		"    line smallint NOT NULL," +
		"    matched_id varchar(36) NOT NULL," +
		"    bib_id varchar(16) NOT NULL," +
		"    tag varchar(3) NOT NULL," +
		"    ind1 varchar(1) NOT NULL," +
		"    ind2 varchar(1) NOT NULL," +
		"    ord smallint NOT NULL," +
		"    sf varchar(1) NOT NULL," +
		"    content varchar(65535) NOT NULL" +
		");"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table: %s", err)
	}
	q = "" +
		"CREATE TABLE IF NOT EXISTS " + tableoutSchema + "." + tablefinalTable + " (" +
		"    srs_id varchar(36) NOT NULL," +
		"    line smallint NOT NULL," +
		"    PRIMARY KEY (srs_id, line)" +
		");"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating temp table: %s", err)
	}
	return nil
}

func transform(txout *sql.Tx, r *reader.Reader) (int64, error) {
	var writeCount int64
	var err error
	// Statement
	var stmt *sql.Stmt
	if txout != nil {
		if stmt, err = txout.PrepareContext(context.TODO(), pq.CopyInSchema(tableoutSchema, tableoutTable,
			"srs_id", "line", "matched_id", "bib_id", "tag", "ind1", "ind2", "ord", "sf", "content")); err != nil {
			return 0, err
		}
	}

	for {
		var next bool
		if next, err = r.Next(printerr); err != nil {
			return 0, err
		}
		if !next {
			break
		}
		var id, matchedID string
		var m *srs.Marc
		id, matchedID, m = r.Values()
		if txout != nil {
			if _, err = stmt.ExecContext(context.TODO(), id, m.Line, matchedID, m.BibID, m.Tag, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content); err != nil {
				return 0, err
			}
		} else {
			fmt.Fprintf(csvFile, "%q,%d,%q,%q,%q,%q,%q,%d,%q,%q\n", id, m.Line, matchedID, m.BibID, m.Tag, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content)
		}
		writeCount++
	}

	if txout != nil {
		if _, err = stmt.ExecContext(context.TODO()); err != nil {
			return 0, err
		}
		if err = stmt.Close(); err != nil {
			return 0, err
		}
	}

	return writeCount, nil
}

func index(txout *sql.Tx) error {
	var err error
	// Index columns
	var cols = []string{"content", "matched_id", "bib_id", "tag", "ind1", "ind2", "ord", "sf"}
	if err = indexColumns(txout, cols); err != nil {
		return err
	}
	// Create primary key
	var q = "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_name = '" + tableoutTable + "_pkey' LIMIT 1;"
	var s string
	if err = txout.QueryRowContext(context.TODO(), q).Scan(&s); err != nil && err != sql.ErrNoRows {
		return err
	}
	var suffix string
	if err != sql.ErrNoRows {
		suffix = "1"
	}
	printerr("creating index: srs_id, line")
	q = "ALTER TABLE " + tableout + " ADD CONSTRAINT " + tableoutTable + "_pkey" + suffix + " PRIMARY KEY (srs_id, line);"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating index: srs_id, line: %s", err)
	}
	return nil
}

func indexColumns(txout *sql.Tx, cols []string) error {
	var err error
	var c string
	for _, c = range cols {
		if c == "content" {
			if !*noTrigramIndexFlag {
				printerr("creating index with pg_trgm extension: %s", c)
				var q = "CREATE INDEX ON " + tableout + " USING GIN (" + c + " gin_trgm_ops);"
				if _, err = txout.ExecContext(context.TODO(), q); err != nil {
					return fmt.Errorf("creating index with pg_trgm extension: %s: %s", c, err)
				}
			}
		} else {
			printerr("creating index: %s", c)
			var q = "CREATE INDEX ON " + tableout + " (" + c + ");"
			if _, err = txout.ExecContext(context.TODO(), q); err != nil {
				return fmt.Errorf("creating index: %s: %s", c, err)
			}
		}
	}
	return nil
}

func replace(txout *sql.Tx) error {
	var err error
	var q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + tablefinalTable + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableout + " RENAME TO " + tablefinalTable + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("renaming table: %s", err)
	}
	return nil
}

func grant(txout *sql.Tx, user string) error {
	var err error
	// Grant permission to LDP user
	var q = "GRANT USAGE ON SCHEMA " + tableoutSchema + " TO " + user + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("schema permission: %s", err)
	}
	q = "GRANT SELECT ON " + tableoutSchema + "." + tablefinalTable + " TO " + user + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("table permission: %s", err)
	}
	return nil
}

func printerr(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, "%s: %s\n", program, fmt.Sprintf(format, v...))
}
