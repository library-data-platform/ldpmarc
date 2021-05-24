package inc

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/reader"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/util"
)

const schemaVersion int64 = 2
const cksumTable = "dbsystem.ldpmarc_cksum"
const metadataTableS = "dbsystem"
const metadataTableT = "ldpmarc_metadata"
const metadataTable = metadataTableS + "." + metadataTableT

func IncUpdateAvail(db *sql.DB) (bool, error) {
	var err error
	// check if metadata table exists
	var q = "SELECT 1 FROM information_schema.tables WHERE table_schema = '" + metadataTableS + "' AND table_name = '" + metadataTableT + "';"
	var i int64
	err = db.QueryRowContext(context.TODO(), q).Scan(&i)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// check if version matches
	q = "SELECT version FROM " + metadataTable + " ORDER BY version LIMIT 1;"
	var v int64
	err = db.QueryRowContext(context.TODO(), q).Scan(&v)
	if err == sql.ErrNoRows {
		return false, fmt.Errorf("version number not found")
	}
	if err != nil {
		return false, err
	}
	if v != schemaVersion {
		return false, nil
	}
	return true, nil
}

func CreateCksum(db *sql.DB, srsRecords, srsMarc string) error {
	var err error
	var tx *sql.Tx
	if tx, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		return err
	}
	defer tx.Rollback()
	// cksum
	var q = "DROP TABLE IF EXISTS " + cksumTable + ";"
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping checksum table: %s", err)
	}
	q = "CREATE TABLE " + cksumTable +
		" AS SELECT r.id, " + util.MD5() + " cksum FROM " + srsRecords + " r JOIN " + srsMarc + " m ON r.id = m.id;"
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating checksum table: %s", err)
	}
	q = "CREATE INDEX ON " + cksumTable + " (id, cksum);"
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("indexing checksum table: %s", err)
	}
	// metadata
	q = "DROP TABLE IF EXISTS " + metadataTable + ";"
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping metadata table: %s", err)
	}
	q = "CREATE TABLE " + metadataTable + " AS SELECT " + strconv.FormatInt(schemaVersion, 10) + " AS version;"
	if _, err = tx.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating metadata table: %s", err)
	}
	// commit
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func VacuumCksum(db *sql.DB) error {
	var err error
	if err = util.VacuumAnalyze(db, cksumTable); err != nil {
		return err
	}
	if err = util.VacuumAnalyze(db, metadataTable); err != nil {
		return err
	}
	return nil
}

func IncUpdate(db *sql.DB, srsRecords, srsMarc, tablefinal string, printerr func(string, ...interface{}), verbose bool) error {
	var err error
	var txout *sql.Tx
	if txout, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		return err
	}
	defer txout.Rollback()
	// add new data
	if err = updateNew(db, srsRecords, srsMarc, tablefinal, txout, printerr, verbose); err != nil {
		return err
	}
	// remove deleted data
	if err = updateDelete(db, srsRecords, srsMarc, tablefinal, txout, printerr, verbose); err != nil {
		return err
	}
	// replace modified data
	if err = updateChange(db, srsRecords, srsMarc, tablefinal, txout, printerr, verbose); err != nil {
		return err
	}
	// commit
	if err = txout.Commit(); err != nil {
		return err
	}
	// vacuum
	printerr("vacuuming")
	if err = util.VacuumAnalyze(db, tablefinal); err != nil {
		return err
	}
	if err = VacuumCksum(db); err != nil {
		return err
	}
	return nil
}

func updateNew(db *sql.DB, srsRecords, srsMarc, tablefinal string, txout *sql.Tx, printerr func(string, ...interface{}), verbose bool) error {
	var err error
	// find new data
	var q = "CREATE TEMP TABLE ldpmarc_add AS SELECT r.id FROM " + srsRecords + " r LEFT JOIN " + cksumTable + " c ON r.id = c.id WHERE c.id IS NULL;"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating addition table: %s", err)
	}
	q = "ALTER TABLE ldpmarc_add ADD CONSTRAINT ldpmarc_add_pkey PRIMARY KEY (id);"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating primary key on addition table: %s", err)
	}
	// txn for select
	var tx *sql.Tx
	if tx, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		return err
	}
	defer tx.Rollback()
	// transform
	q = filterQuery(srsRecords, srsMarc, "ldpmarc_add")
	var rows *sql.Rows
	if rows, err = tx.QueryContext(context.TODO(), q); err != nil {
		return fmt.Errorf("selecting records to add: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var idN, matchedIDN, instanceHRIDN, stateN, dataN sql.NullString
		var cksum string
		if err = rows.Scan(&idN, &matchedIDN, &instanceHRIDN, &stateN, &dataN, &cksum); err != nil {
			return err
		}
		var inrec = reader.Record{
			Stop:         false,
			Err:          nil,
			ID:           idN,
			MatchedID:    matchedIDN,
			InstanceHRID: instanceHRIDN,
			State:        stateN,
			Data:         dataN,
		}
		var id, matchedID, instanceHRID, instanceID string
		var mrecs []srs.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(inrec, printerr, verbose)
		if skip {
			continue
		}
		var m srs.Marc
		for _, m = range mrecs {
			q = "INSERT INTO " + tablefinal + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);"
			if _, err = txout.ExecContext(context.TODO(), q,
				id, m.Line, matchedID, instanceHRID, instanceID, m.Field, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content); err != nil {
				return fmt.Errorf("adding record: %s", err)
			}
		}
		// cksum
		q = "INSERT INTO " + cksumTable + " VALUES ($1, $2);"
		if _, err = txout.ExecContext(context.TODO(), q, id, cksum); err != nil {
			return fmt.Errorf("adding record: %s", err)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return nil
}

func updateDelete(db *sql.DB, srsRecords, srsMarc, tablefinal string, txout *sql.Tx, printerr func(string, ...interface{}), verbose bool) error {
	var err error
	// find new data
	var q = "CREATE TEMP TABLE ldpmarc_delete AS SELECT c.id FROM " + srsRecords + " r RIGHT JOIN " + cksumTable + " c ON r.id = c.id WHERE r.id IS NULL;"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating deletion table: %s", err)
	}
	q = "ALTER TABLE ldpmarc_delete ADD CONSTRAINT ldpmarc_delete_pkey PRIMARY KEY (id);"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating primary key on deletion table: %s", err)
	}
	// show changes
	if verbose {
		q = "SELECT id FROM ldpmarc_delete;"
		var rows *sql.Rows
		if rows, err = txout.QueryContext(context.TODO(), q); err != nil {
			return fmt.Errorf("reading deletion list: %s", err)
		}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				return fmt.Errorf("reading deletion ID: %s", err)
			}
			printerr("removing: id=%s", id)
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf("reading deletion rows: %s", err)
		}
		rows.Close()
	}
	// delete in finaltable
	q = "DELETE FROM " + tablefinal + " WHERE srs_id IN (SELECT id FROM ldpmarc_delete);"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("deleting records: %s", err)
	}
	// delete in cksum table
	q = "DELETE FROM " + cksumTable + " WHERE id IN (SELECT id FROM ldpmarc_delete);"
	if _, err = txout.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("deleting cksum: %s", err)
	}
	return nil
}

func updateChange(db *sql.DB, srsRecords, srsMarc, tablefinal string, txout *sql.Tx, printerr func(string, ...interface{}), verbose bool) error {
	var err error
	// find changed data
	var q = "CREATE TEMP TABLE ldpmarc_change AS SELECT r.id FROM " + srsRecords + " r JOIN " + cksumTable + " c ON r.id = c.id JOIN " + srsMarc + " m ON r.id = m.id WHERE " + util.MD5() + " <> c.cksum;"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating change table: %s", err)
	}
	q = "ALTER TABLE ldpmarc_change ADD CONSTRAINT ldpmarc_change_pkey PRIMARY KEY (id);"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("creating primary key on change table: %s", err)
	}
	// txn for select
	var tx *sql.Tx
	if tx, err = db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		return err
	}
	defer tx.Rollback()
	// transform
	q = filterQuery(srsRecords, srsMarc, "ldpmarc_change")
	var rows *sql.Rows
	if rows, err = tx.QueryContext(context.TODO(), q); err != nil {
		return fmt.Errorf("selecting records to change: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var idN, matchedIDN, instanceHRIDN, stateN, dataN sql.NullString
		var cksum string
		if err = rows.Scan(&idN, &matchedIDN, &instanceHRIDN, &stateN, &dataN, &cksum); err != nil {
			return err
		}
		var inrec = reader.Record{
			Stop:         false,
			Err:          nil,
			ID:           idN,
			MatchedID:    matchedIDN,
			InstanceHRID: instanceHRIDN,
			State:        stateN,
			Data:         dataN,
		}
		var id, matchedID, instanceHRID, instanceID string
		var mrecs []srs.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(inrec, printerr, verbose)
		if skip {
			continue
		}
		// delete in finaltable
		q = "DELETE FROM " + tablefinal + " WHERE srs_id = '" + id + "';"
		if _, err = txout.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("deleting record (change): %s", err)
		}
		// delete in cksum table
		q = "DELETE FROM " + cksumTable + " WHERE id = '" + id + "';"
		if _, err = txout.ExecContext(context.TODO(), q); err != nil {
			return fmt.Errorf("deleting cksum (change): %s", err)
		}
		var m srs.Marc
		for _, m = range mrecs {
			q = "INSERT INTO " + tablefinal + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);"
			if _, err = txout.ExecContext(context.TODO(), q,
				id, m.Line, matchedID, instanceHRID, instanceID, m.Field, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content); err != nil {
				return fmt.Errorf("rewriting record: %s", err)
			}
		}
		// cksum
		q = "INSERT INTO " + cksumTable + " VALUES ($1, $2);"
		if _, err = txout.ExecContext(context.TODO(), q, id, cksum); err != nil {
			return fmt.Errorf("rewriting record: %s", err)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return nil
}

func filterQuery(srsRecords, srsMarc, filter string) string {
	return "" +
		"SELECT r.id, r.matched_id, r.instance_hrid, r.state, m.data, " + util.MD5() + " cksum " +
		"    FROM " + srsRecords + " r " +
		"        JOIN " + filter + " f ON r.id = f.id " +
		"        JOIN " + srsMarc + " m ON r.id = m.id;"
}