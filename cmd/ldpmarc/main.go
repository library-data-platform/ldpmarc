package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/lib/pq"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/inc"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/reader"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/util"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/writer"
	"github.com/spf13/viper"
)

//var fullUpdateFlag = flag.Bool("f", false, "Force full update, even if incremental update is available")
var incUpdateFlag = flag.Bool("i", false, "Use incremental update if possible (experimental)")
var datadirFlag = flag.String("D", "", "LDP data directory")
var ldpUserFlag = flag.String("u", "", "LDP user to be granted select privileges")
var noParallelVacuumFlag = flag.Bool("P", false, "Disable parallel vacuum (PostgreSQL 13 or later only)")
var noTrigramIndexFlag = flag.Bool("T", false, "Disable creation of trigram indexes")
var verboseFlag = flag.Bool("v", false, "Enable verbose output")
var csvFilenameFlag = flag.String("c", "", "Write output to CSV file instead of a database")
var helpFlag = flag.Bool("h", false, "Help for ldpmarc")

var srsRecords = "public.srs_records"
var srsMarc = "public.srs_marc"
var tableoutSchema = "public"
var tableoutTable = "_srs_marctab"
var tableout = tableoutSchema + "." + tableoutTable
var tablefinalTable = "srs_marctab"
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
	if *incUpdateFlag && incUpdateAvail /* && !*fullUpdateFlag */ && *csvFilenameFlag == "" {
		printerr("incremental update (experimental)")
		if err = inc.IncUpdate(db, srsRecords, srsMarc, tablefinal, *noParallelVacuumFlag, printerr, *verboseFlag); err != nil {
			return err
		}
	} else {
		if err = fullUpdate(db); err != nil {
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
		printerr("new table is ready to use: " + tablefinal)
		printerr("writing checksums")
		if err = inc.CreateCksum(db, srsRecords, srsMarc); err != nil {
			return err
		}
		printerr("vacuuming")
		if err = util.VacuumAnalyze(db, tablefinal, *noParallelVacuumFlag); err != nil {
			return err
		}
		if err = inc.VacuumCksum(db, *noParallelVacuumFlag); err != nil {
			return err
		}
	}
	return nil
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
	// read number of input records
	var inputCount int64
	if inputCount, err = selectCount(txin, srsRecords); err != nil {
		return err
	}
	printerr("transforming %d input records", inputCount)
	// main processing
	var rch <-chan reader.Record = reader.ReadAll(txin, srsRecords, srsMarc, *verboseFlag)
	var writeCount int64
	if writeCount, err = processAll(txout, rch); err != nil {
		return err
	}
	// Commit
	if err = txin.Rollback(); err != nil {
		return err
	}
	printerr("%d output rows", writeCount)
	return nil
}

func setupTable(txout *sql.Tx) error {
	var err error
	var q string
	if tableoutSchema != "public" {
		q = "CREATE SCHEMA IF NOT EXISTS " + tableoutSchema + ";"
		if _, err = txout.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("creating schema: %s", err)
		}
	}
	q = "" +
		"CREATE TABLE " + tableout + " (" +
		"    srs_id uuid NOT NULL," +
		"    line smallint NOT NULL," +
		"    matched_id uuid NOT NULL," +
		"    instance_hrid varchar(32) NOT NULL," +
		"    instance_id uuid NOT NULL," +
		"    field varchar(3) NOT NULL," +
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
		"    srs_id uuid NOT NULL," +
		"    line smallint NOT NULL," +
		"    PRIMARY KEY (srs_id, line)" +
		");"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating temp table: %s", err)
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

func processAll(txout *sql.Tx, rch <-chan reader.Record) (int64, error) {
	var writeCount int64
	var err error
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
				fmt.Fprintf(csvFile, "%q,%d,%q,%q,%q,%q,%q,%q,%d,%q,%q\n", id, m.Line, matchedID, instanceHRID, instanceID, m.Field, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content)
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

	return writeCount, nil
}

func index(txout *sql.Tx) error {
	var err error
	// Index columns
	var cols = []string{"content", "matched_id", "instance_hrid", "instance_id", "field", "ind1", "ind2", "ord", "sf"}
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
				printerr("creating index: %s", c)
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
	var q = "DROP TABLE IF EXISTS folio_source_record.__marc;"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + tablefinalTable + ";"
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
