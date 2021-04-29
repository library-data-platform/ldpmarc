package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/reader"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
	"github.com/spf13/viper"
)

var odbcFilenameFlag = flag.String("f", "", "ODBC data source file name (e.g. \"$HOME/.odbc.ini\")")
var odbcDSNFlag = flag.String("d", "", "ODBC data source name (DSN)")
var ldpUserFlag = flag.String("u", "", "LDP user to be granted select privileges")
var verboseFlag = flag.Bool("v", false, "Enable verbose output")
var csvFilenameFlag = flag.String("c", "", "Write output to CSV file instead of a database")
var helpFlag = flag.Bool("h", false, "Help for ldpmarc")

var tablein = "public.srs_marc"
var tableoutSchema = "folio_source_record"
var tableoutTable = "__srs_marc"
var tableout = tableoutSchema + "." + tableoutTable
var tablefinal = "__marc"

var odbcFilename string
var odbcDSN string
var ldpUser string
var verbose bool
var csvFilename string
var csvFile *os.File

var program = "ldpmarc"

func main() {
	flag.Parse()
	if len(flag.Args()) > 0 {
		printerr("invalid argument: %s", flag.Arg(0))
		os.Exit(2)
	}
	if *helpFlag || *odbcFilenameFlag == "" || *odbcDSNFlag == "" || (*ldpUserFlag == "" && *csvFilenameFlag == "") {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", program)
		flag.PrintDefaults()
		if *helpFlag {
			return
		} else {
			os.Exit(2)
		}
	}
	odbcFilename = *odbcFilenameFlag
	odbcDSN = *odbcDSNFlag
	ldpUser = *ldpUserFlag
	verbose = *verboseFlag
	csvFilename = *csvFilenameFlag
	var err error
	if err = run(); err != nil {
		printerr("%s", err)
		os.Exit(1)
	}
	printerr("completed")
}

func run() error {
	// Read database configuration
	viper.SetConfigFile(odbcFilename)
	viper.SetConfigType("ini")
	var err error
	var ok bool
	if err = viper.ReadInConfig(); err != nil {
		if _, ok = err.(viper.ConfigFileNotFoundError); ok {
			return fmt.Errorf("file not found: %s", odbcFilename)
		} else {
			return fmt.Errorf("error reading file: %s: %s", odbcFilename, err)
		}
	}
	var host = viper.GetString(odbcDSN + ".Servername")
	var port = viper.GetString(odbcDSN + ".Port")
	var user = viper.GetString(odbcDSN + ".UserName")
	var password = viper.GetString(odbcDSN + ".Password")
	var dbname = viper.GetString(odbcDSN + ".Database")
	var sslmode = viper.GetString(odbcDSN + ".SSLMode")
	// Open database
	var db *sql.DB
	if db, err = openDB(host, port, user, password, dbname, sslmode); err != nil {
		return err
	}
	// Begin output transaction
	var txout *sql.Tx
	if csvFilename == "" {
		if txout, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
			return err
		}
		defer txout.Rollback()
	} else {
		if csvFile, err = os.Create(csvFilename); err != nil {
			return err
		}
		defer csvFile.Close()
		printerr("output will be written to file: %s", csvFilename)
	}
	// Process MARC data
	if err = process(db, txout); err != nil {
		return err
	}
	if csvFilename == "" {
		// Index columns
		if err = index(txout); err != nil {
			return err
		}
		// Replace table
		if err = replace(txout); err != nil {
			return err
		}
		// Grant permission to LDP user
		if err = grant(txout, ldpUser); err != nil {
			return err
		}
		// Commit
		if err = txout.Commit(); err != nil {
			return err
		}
		printerr("new table \"" + tableoutSchema + "." + tablefinal + "\" is ready to use")
		// Vacuum
		printerr("vacuuming")
		var q = "VACUUM ANALYZE " + tableoutSchema + "." + tablefinal + ";"
		if _, err = db.ExecContext(context.TODO(), q); err != nil {
			return qerror(err, q)
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
	printerr("reading table \"%s\"", tablein)
	var r *reader.Reader
	var inputCount int64
	if r, inputCount, err = reader.NewReader(txin, tablein, verbose); err != nil {
		return err
	} // Deferred r.Close() causes process to hang
	printerr("processing %d input records", inputCount)
	var writeCount int64
	if _, writeCount, err = transform(txout, r); err != nil {
		return err
	}
	r.Close()
	// Commit
	if err = txin.Rollback(); err != nil {
		return err
	}
	printerr("%d output records", writeCount)
	return nil
}

func setupTable(txout *sql.Tx) error {
	var err error
	var q = "CREATE SCHEMA IF NOT EXISTS " + tableoutSchema + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return qerror(err, q)
	}
	q = "" +
		"CREATE TABLE " + tableout + " (" +
		"    id varchar(36) NOT NULL," +
		"    line smallint NOT NULL," +
		"    bib_id varchar(16) NOT NULL," +
		"    tag varchar(3) NOT NULL," +
		"    ind1 varchar(1) NOT NULL," +
		"    ind2 varchar(1) NOT NULL," +
		"    ord smallint NOT NULL," +
		"    sf varchar(1) NOT NULL," +
		"    content varchar(65535) NOT NULL" +
		");"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return qerror(err, q)
	}
	q = "" +
		"CREATE TABLE IF NOT EXISTS " + tableoutSchema + "." + tablefinal + " (" +
		"    id varchar(36) NOT NULL," +
		"    line smallint NOT NULL," +
		"    PRIMARY KEY (id, line)" +
		");"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return qerror(err, q)
	}
	return nil
}

func transform(txout *sql.Tx, r *reader.Reader) (int64, int64, error) {
	var writeCount int64
	var err error
	// Statement
	var stmt *sql.Stmt
	if txout != nil {
		if stmt, err = txout.PrepareContext(context.TODO(), pq.CopyInSchema(tableoutSchema, tableoutTable,
			"id", "line", "bib_id", "tag", "ind1", "ind2", "ord", "sf", "content")); err != nil {
			return 0, 0, err
		}
	}

	for {
		var next bool
		if next, err = r.Next(printerr); err != nil {
			return 0, 0, err
		}
		if !next {
			break
		}
		var id string
		var m *srs.Marc
		id, m = r.Values()
		if txout != nil {
			if _, err = stmt.ExecContext(context.TODO(), id, m.Line, m.BibID, m.Tag, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content); err != nil {
				return 0, 0, err
			}
		} else {
			fmt.Fprintf(csvFile, "%q,%d,%q,%q,%q,%q,%d,%q,%q\n", id, m.Line, m.BibID, m.Tag, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content)
		}
		writeCount++
	}

	if txout != nil {
		if _, err = stmt.ExecContext(context.TODO()); err != nil {
			return 0, 0, err
		}
		if err = stmt.Close(); err != nil {
			return 0, 0, err
		}
	}

	return r.ReadCount(), writeCount, nil
}

func index(txout *sql.Tx) error {
	var err error
	// Create primary key
	printerr("creating indexes")
	var q = "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_name = '" + tableoutTable + "_pkey' LIMIT 1;"
	var s string
	if err = txout.QueryRowContext(context.TODO(), q).Scan(&s); err != nil && err != sql.ErrNoRows {
		return err
	}
	var suffix string
	if err != sql.ErrNoRows {
		suffix = "1"
	}
	q = "ALTER TABLE " + tableout + " ADD CONSTRAINT " + tableoutTable + "_pkey" + suffix + " PRIMARY KEY (id, line);"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return qerror(err, q)
	}
	// Index columns
	var cols = []string{"bib_id", "tag", "ind1", "ind2", "ord", "sf"}
	if err = indexColumns(txout, cols); err != nil {
		return err
	}
	return nil
}

func indexColumns(txout *sql.Tx, cols []string) error {
	var err error
	var c string
	var x int
	for x, c = range cols {
		var progress = int(float64(x+1) / float64(len(cols)+1) * 100)
		if progress > 0 {
			printerr("creating indexes: %d%%", progress)
		}
		var q = "CREATE INDEX ON " + tableout + " (" + c + ");"
		if _, err = txout.ExecContext(context.TODO(), q); err != nil {
			return qerror(err, q)
		}
	}
	printerr("creating indexes: 100%%")
	return nil
}

func replace(txout *sql.Tx) error {
	var err error
	var q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + tablefinal + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return qerror(err, q)
	}
	q = "ALTER TABLE " + tableout + " RENAME TO " + tablefinal + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return qerror(err, q)
	}
	return nil
}

func grant(txout *sql.Tx, user string) error {
	var err error
	// Grant permission to LDP user
	var q = "GRANT USAGE ON SCHEMA " + tableoutSchema + " TO " + user + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return qerror(err, q)
	}
	q = "GRANT SELECT ON " + tableoutSchema + "." + tablefinal + " TO " + user + ";"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return qerror(err, q)
	}
	return nil
}

func qerror(err error, q string) error {
	return fmt.Errorf("%s: %s", err, q)
}

func printerr(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, "%s: %s\n", program, fmt.Sprintf(format, v...))
}
