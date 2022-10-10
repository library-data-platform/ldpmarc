package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/jackc/pgx/v4"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/inc"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/local"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/util"
	"github.com/spf13/viper"
	"gopkg.in/ini.v1"
)

var fullUpdateFlag = flag.Bool("f", false, "Perform full update even if incremental update is available")
var incUpdateFlag = flag.Bool("i", false, "[option no longer supported]")
var datadirFlag = flag.String("D", "", "Data directory")
var ldpUserFlag = flag.String("u", "", "User to be granted select privileges")
var noTrigramIndexFlag = flag.Bool("T", false, "Disable creation of trigram indexes")
var verboseFlag = flag.Bool("v", false, "Enable verbose output")
var csvFilenameFlag = flag.String("c", "", "Write output to CSV file instead of a database")
var srsRecordsFlag = flag.String("r", "", "Name of table containing SRS records to read")
var srsMarcFlag = flag.String("m", "", "Name of table containing SRS MARC (JSON) data to read")
var srsMarcAttrFlag = flag.String("j", "", "Name of column containing MARC JSON data")
var metadbFlag = flag.Bool("M", false, "Metadb compatibility")
var helpFlag = flag.Bool("h", false, "Help for ldpmarc")

var tableoutSchema = "marctab"
var tableoutTable = "_mt"
var tableout = tableoutSchema + "." + tableoutTable
var tablefinalSchema = "folio_source_record"
var tablefinalTable = "marctab"
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
	if *incUpdateFlag {
		printerr("-i option no longer supported")
		os.Exit(1)
	}
	if !*metadbFlag { // LDP1
		tablefinalSchema = "public"
		tablefinalTable = "srs_marctab"
	}
	loc := setupLocations()
	if err := run(loc); err != nil {
		printerr("%s", err)
		os.Exit(1)
	}
}

func run(loc *locations) error {
	// Read database configuration
	var host, port, user, password, dbname, sslmode string
	var err error
	if *metadbFlag {
		host, port, user, password, dbname, sslmode, err = readConfigMetadb()
		if err != nil {
			return err
		}
	} else {
		host, port, user, password, dbname, sslmode, err = readConfigLDP1()
		if err != nil {
			return err
		}
	}
	var dbc = new(util.DBC)
	dbc.ConnString = "host=" + host + " port=" + port + " user=" + user + " password=" + password + " dbname=" + dbname + " sslmode=" + sslmode
	if dbc.Conn, err = pgx.Connect(context.TODO(), dbc.ConnString); err != nil {
		return err
	}
	defer dbc.Conn.Close(context.TODO())
	if err = setupSchema(dbc); err != nil {
		return fmt.Errorf("setting up schema: %v", err)
	}
	var incUpdateAvail bool
	if incUpdateAvail, err = inc.IncUpdateAvail(dbc); err != nil {
		return err
	}
	if incUpdateAvail && !*fullUpdateFlag && *csvFilenameFlag == "" {
		printerr("incremental update")
		if err = inc.IncUpdate(dbc, loc.SrsRecords, loc.SrsMarc, loc.SrsMarcAttr, tablefinal, printerr, *verboseFlag); err != nil {
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
				var conn *pgx.Conn
				conn, err = pgx.Connect(context.TODO(), dbc.ConnString)
				if err == nil {
					_, _ = conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout)
					for _, field := range allFields {
						_, _ = conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout+field)
					}
					conn.Close(context.TODO())
				}
				os.Exit(130)
			}
		}()
		// Run full update
		if err = fullUpdate(loc, dbc); err != nil {
			var err2 error
			var conn *pgx.Conn
			conn, err2 = pgx.Connect(context.TODO(), dbc.ConnString)
			if err2 == nil {
				_, _ = conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout)
				conn.Close(context.TODO())
			}
			return err
		}
	}
	return nil
}

func setupSchema(dbc *util.DBC) error {
	var err error
	if _, err = dbc.Conn.Exec(context.TODO(), "CREATE SCHEMA IF NOT EXISTS "+tableoutSchema); err != nil {
		return fmt.Errorf("creating schema: %s", err)
	}
	var q = "COMMENT ON SCHEMA " + tableoutSchema + " IS 'system tables for SRS MARC transform'"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("adding comment on schema: %s", err)
	}
	return nil
}

func fullUpdate(loc *locations, dbc *util.DBC) error {
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
	inputCount, err := process(loc, dbc)
	if err != nil {
		return err
	}
	if *csvFilenameFlag == "" {
		// Index columns
		if err = index(dbc); err != nil {
			return err
		}
		// Replace table
		if err = replace(dbc); err != nil {
			return err
		}
		// Grant permission to LDP user
		if ldpUserFlag != nil && *ldpUserFlag != "" {
			if err = grant(dbc, *ldpUserFlag); err != nil {
				return err
			}
		}
		_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS dbsystem.ldpmarc_cksum;")
		_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS dbsystem.ldpmarc_metadata;")
		if inputCount > 0 {
			printerr("writing checksums")
			if err = inc.CreateCksum(dbc, loc.SrsRecords, loc.SrsMarc, tablefinal, loc.SrsMarcAttr); err != nil {
				return err
			}
		}
		printerr("vacuuming")
		if err = util.VacuumAnalyze(dbc, tablefinal); err != nil {
			return err
		}
		if err = inc.VacuumCksum(dbc); err != nil {
			return err
		}
		printerr("new table is ready to use: " + tablefinal)
	}
	return nil
}

func process(loc *locations, dbc *util.DBC) (int64, error) {
	var err error
	var store *local.Store
	if store, err = local.NewStore(*datadirFlag); err != nil {
		return 0, err
	}
	defer store.Close()
	if err = setupTables(dbc); err != nil {
		return 0, err
	}

	var inputCount int64
	if inputCount, err = selectCount(dbc, loc.SrsRecords); err != nil {
		return 0, err
	}
	printerr("transforming %d input records", inputCount)
	// main processing
	var writeCount int64
	if inputCount > 0 {
		if writeCount, err = processAll(loc, dbc, store); err != nil {
			return 0, err
		}
	}
	printerr("%d output rows", writeCount)
	return inputCount, nil
}

func setupTables(dbc *util.DBC) error {
	var err error
	var q string
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout)
	if !*noTrigramIndexFlag && !util.IsTrgmAvailable(dbc) {
		return fmt.Errorf("unable to access pg_trgm module extension")
	}
	var lz4 string
	if util.IsLZ4Available(dbc) {
		lz4 = " COMPRESSION lz4"
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
		"    content varchar(65535)" + lz4 + " NOT NULL" +
		") PARTITION BY LIST (field);"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating table: %s", err)
	}
	q = "COMMENT ON TABLE " + tableout + " IS 'current SRS MARC records in tabular form'"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("adding comment on table: %s", err)
	}
	for _, field := range allFields {
		_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.srs_marctab_"+field)
		_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableout+field)
		q = "CREATE TABLE " + tableout + field +
			" PARTITION OF " + tableout + " FOR VALUES IN ('" + field + "');"
		if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("creating partition: %s", err)
		}
	}
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.cksum")
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS ldpmarc.metadata")
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP SCHEMA IF EXISTS ldpmarc")
	return nil
}

func selectCount(dbc *util.DBC, tablein string) (int64, error) {
	var err error
	var count int64
	var q = "SELECT count(*) FROM " + tablein + ";"
	if err = dbc.Conn.QueryRow(context.TODO(), q).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func processAll(loc *locations, dbc *util.DBC, store *local.Store) (int64, error) {
	var err error
	var msg *string
	var writeCount int64
	var q = "SELECT r.id, r.matched_id, r.external_hrid instance_hrid, r.state, m." + loc.SrsMarcAttr + " FROM " + loc.SrsRecords + " r JOIN " + loc.SrsMarc + " m ON r.id = m.id"
	var rows pgx.Rows
	if rows, err = dbc.Conn.Query(context.TODO(), q); err != nil {
		return 0, fmt.Errorf("selecting marc records: %v", err)
	}
	for rows.Next() {
		var id, matchedID, instanceHRID, state, data *string
		if err = rows.Scan(&id, &matchedID, &instanceHRID, &state, &data); err != nil {
			return 0, fmt.Errorf("scanning records: %v", err)
		}
		var record local.Record
		var instanceID string
		var mrecs []srs.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(id, matchedID, instanceHRID, state, data, printerr, *verboseFlag)
		if skip {
			continue
		}
		var m srs.Marc
		for _, m = range mrecs {
			if *csvFilenameFlag == "" {
				record.SRSID = *id
				record.Line = m.Line
				record.MatchedID = *matchedID
				record.InstanceHRID = *instanceHRID
				record.InstanceID = instanceID
				record.Field = m.Field
				record.Ind1 = m.Ind1
				record.Ind2 = m.Ind2
				record.Ord = m.Ord
				record.SF = m.SF
				record.Content = m.Content
				msg, err = store.Write(&record)
				if err != nil {
					return 0, fmt.Errorf("writing record: %v: %v", err, record)
				}
				if msg != nil {
					printerr("skipping line in record: %s: %s", *id, *msg)
					continue
				}
				writeCount++
			} else {
				_, _ = fmt.Fprintf(csvFile, "%q,%d,%q,%q,%q,%q,%q,%q,%d,%q,%q\n", *id, m.Line, *matchedID, *instanceHRID, instanceID, m.Field, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content)
				writeCount++
			}
		}
	}
	if rows.Err() != nil {
		return 0, fmt.Errorf("row error: %v", rows.Err())
	}
	rows.Close()

	if err = store.FinishWriting(); err != nil {
		return 0, err
	}

	var f string
	for _, f = range allFields {
		src, err := store.ReadSource(f, printerr)
		if err != nil {
			return 0, err
		}
		_, err = dbc.Conn.CopyFrom(context.TODO(),
			pgx.Identifier{tableoutSchema, tableoutTable + f},
			[]string{"srs_id", "line", "matched_id", "instance_hrid", "instance_id", "field", "ind1", "ind2", "ord", "sf", "content"},
			src)
		if err != nil {
			return 0, fmt.Errorf("copying to database: %v", err)
		}
		src.Close()
	}

	return writeCount, nil
}

func index(dbc *util.DBC) error {
	var err error
	// Index columns
	var cols = []string{"content", "matched_id", "instance_hrid", "instance_id", "ind1", "ind2", "ord", "sf"}
	if err = indexColumns(dbc, cols); err != nil {
		return err
	}
	// Create unique index
	printerr("creating index: srs_id, line, field")
	var q = "CREATE UNIQUE INDEX ON " + tableout + " (srs_id, line, field);"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating index: srs_id, line, field: %s", err)
	}
	return nil
}

func indexColumns(dbc *util.DBC, cols []string) error {
	var err error
	var c string
	for _, c = range cols {
		if c == "content" {
			if !*noTrigramIndexFlag {
				printerr("creating index: %s", c)
				var q = "CREATE INDEX ON " + tableout + " USING GIN (" + c + " gin_trgm_ops)"
				if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
					return fmt.Errorf("creating index with pg_trgm extension: %s: %s", c, err)
				}
			}
		} else {
			printerr("creating index: %s", c)
			var q = "CREATE INDEX ON " + tableout + " (" + c + ")"
			if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
				return fmt.Errorf("creating index: %s: %s", c, err)
			}
		}
	}
	return nil
}

func replace(dbc *util.DBC) error {
	var err error
	var q = "DROP TABLE IF EXISTS folio_source_record.__marc"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + tablefinalTable
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableout + " RENAME TO " + tablefinalTable
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("renaming table: %s", err)
	}
	q = "DROP TABLE IF EXISTS " + tablefinal
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping table: %s", err)
	}
	q = "ALTER TABLE " + tableoutSchema + "." + tablefinalTable + " SET SCHEMA " + tablefinalSchema
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("moving table: %s", err)
	}
	for _, field := range allFields {
		q = "DROP TABLE IF EXISTS " + tableoutSchema + "." + tablefinalTable + field
		if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("dropping table: %s", err)
		}
		q = "ALTER TABLE " + tableout + field + " RENAME TO mt" + field
		if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("renaming table: %s", err)
		}
	}
	return nil
}

func grant(dbc *util.DBC, user string) error {
	var err error
	// Grant permission to LDP user
	var q = "GRANT USAGE ON SCHEMA " + tablefinalSchema + " TO " + user
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("schema permission: %s", err)
	}
	q = "GRANT SELECT ON " + tablefinal + " TO " + user
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("table permission: %s", err)
	}
	return nil
}

func setupLocations() *locations {
	loc := &locations{
		SrsRecords:  "folio_source_record.records_lb",
		SrsMarc:     "folio_source_record.marc_records_lb",
		SrsMarcAttr: "content",
	}
	if !*metadbFlag { // LDP1
		loc.SrsRecords = "public.srs_records"
		loc.SrsMarc = "public.srs_marc"
		loc.SrsMarcAttr = "data"
	}
	if *srsRecordsFlag != "" {
		loc.SrsRecords = *srsRecordsFlag
	}
	if *srsMarcFlag != "" {
		loc.SrsMarc = *srsMarcFlag
	}
	if *srsMarcAttrFlag != "" {
		loc.SrsMarcAttr = *srsMarcAttrFlag
	}
	return loc
}

type locations struct {
	SrsRecords  string
	SrsMarc     string
	SrsMarcAttr string
}

func readConfigMetadb() (string, string, string, string, string, string, error) {
	var mdbconf = filepath.Join(*datadirFlag, "metadb.conf")
	cfg, err := ini.Load(mdbconf)
	if err != nil {
		return "", "", "", "", "", "", nil
	}
	s := cfg.Section("main")
	host := s.Key("host").String()
	port := s.Key("port").String()
	user := s.Key("systemuser").String()
	password := s.Key("systemuser_password").String()
	dbname := s.Key("database").String()
	sslmode := s.Key("sslmode").String()
	return host, port, user, password, dbname, sslmode, nil
}

func readConfigLDP1() (string, string, string, string, string, string, error) {
	var ldpconf = filepath.Join(*datadirFlag, "ldpconf.json")
	viper.SetConfigFile(ldpconf)
	viper.SetConfigType("json")
	var ok bool
	if err := viper.ReadInConfig(); err != nil {
		if _, ok = err.(viper.ConfigFileNotFoundError); ok {
			return "", "", "", "", "", "", fmt.Errorf("file not found: %s", ldpconf)
		} else {
			return "", "", "", "", "", "", fmt.Errorf("error reading file: %s: %s", ldpconf, err)
		}
	}
	var ldp = "ldp_database"
	var host = viper.GetString(ldp + ".database_host")
	var port = strconv.Itoa(viper.GetInt(ldp + ".database_port"))
	var user = viper.GetString(ldp + ".database_user")
	var password = viper.GetString(ldp + ".database_password")
	var dbname = viper.GetString(ldp + ".database_name")
	var sslmode = viper.GetString(ldp + ".database_sslmode")
	return host, port, user, password, dbname, sslmode, nil
}

func printerr(format string, v ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", program, fmt.Sprintf(format, v...))
}
