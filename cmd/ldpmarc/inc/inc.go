package inc

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v4"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/util"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/uuid"
)

const schemaVersion int64 = 12
const cksumTable = "marctab.cksum"
const metadataTableS = "marctab"
const metadataTableT = "metadata"
const metadataTable = metadataTableS + "." + metadataTableT

func IncUpdateAvail(dbc *util.DBC) (bool, error) {
	var err error
	// check if metadata table exists
	var q = "SELECT 1 FROM information_schema.tables WHERE table_schema = '" + metadataTableS + "' AND table_name = '" + metadataTableT + "';"
	var i int64
	err = dbc.Conn.QueryRow(context.TODO(), q).Scan(&i)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// check if version matches
	q = "SELECT version FROM " + metadataTable + " ORDER BY version LIMIT 1;"
	var v int64
	err = dbc.Conn.QueryRow(context.TODO(), q).Scan(&v)
	if err == pgx.ErrNoRows {
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

func CreateCksum(dbc *util.DBC, srsRecords, srsMarc, srsMarctab, srsMarcAttr string) error {
	var err error
	var tx pgx.Tx
	if tx, err = util.BeginTx(context.TODO(), dbc.Conn); err != nil {
		return err
	}
	defer tx.Rollback(context.TODO())
	// cksum
	var q = "DROP TABLE IF EXISTS " + cksumTable
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping checksum table: %s", err)
	}
	// Filter should match srs.getInstanceID()
	q = "CREATE TABLE " + cksumTable + " (id uuid NOT NULL,cksum text) WITH (fillfactor=80)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating checksum table: %s", err)
	}
	q = "INSERT INTO " + cksumTable + " (id,cksum)" +
		" SELECT r.id::uuid, " + util.MD5(srsMarcAttr) + " cksum FROM " +
		srsRecords + " r JOIN " + srsMarc + " m ON r.id = m.id JOIN " +
		srsMarctab + " mt ON r.id::uuid = mt.srs_id WHERE r.state = 'ACTUAL' AND mt.field = '999' AND mt.sf = 'i' AND ind1 = 'f' AND ind2 = 'f' AND mt.content <> ''"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("writing data to checksum table: %s", err)
	}
	q = "ALTER TABLE " + cksumTable + " ADD CONSTRAINT cksum_pkey PRIMARY KEY (id) WITH (fillfactor=80)"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("indexing checksum table: %s", err)
	}
	// metadata
	q = "DROP TABLE IF EXISTS " + metadataTable
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("dropping metadata table: %s", err)
	}
	q = "CREATE TABLE " + metadataTable + " AS SELECT " + strconv.FormatInt(schemaVersion, 10) + " AS version;"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating metadata table: %s", err)
	}
	// commit
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	return nil
}

func VacuumCksum(dbc *util.DBC) error {
	var err error
	if err = util.VacuumAnalyze(dbc, cksumTable); err != nil {
		return err
	}
	if err = util.VacuumAnalyze(dbc, metadataTable); err != nil {
		return err
	}
	return nil
}

func IncUpdate(dbc *util.DBC, srsRecords, srsMarc, srsMarcAttr, tablefinal string, printerr func(string, ...any), verbose bool) error {
	var err error
	// add new data
	if err = updateNew(dbc, srsRecords, srsMarc, srsMarcAttr, tablefinal, printerr, verbose); err != nil {
		return fmt.Errorf("update new: %s", err)
	}
	// remove deleted data
	if err = updateDelete(dbc, srsRecords, tablefinal, printerr, verbose); err != nil {
		return fmt.Errorf("update delete: %s", err)
	}
	// replace modified data
	if err = updateChange(dbc, srsRecords, srsMarc, srsMarcAttr, tablefinal, printerr, verbose); err != nil {
		return fmt.Errorf("update change: %s", err)
	}
	// vacuum
	printerr("vacuuming")
	if err = util.VacuumAnalyze(dbc, tablefinal); err != nil {
		return fmt.Errorf("vacuum analyze: %s", err)
	}
	if err = VacuumCksum(dbc); err != nil {
		return fmt.Errorf("vacuum cksum: %s", err)
	}
	return nil
}

func updateNew(dbc *util.DBC, srsRecords, srsMarc, srsMarcAttr, tablefinal string, printerr func(string, ...any), verbose bool) error {
	var err error
	// find new data
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS marctab.inc_add")
	var q = "CREATE UNLOGGED TABLE marctab.inc_add AS SELECT r.id::uuid FROM " + srsRecords + " r LEFT JOIN " + cksumTable + " c ON r.id::uuid = c.id WHERE c.id IS NULL;"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating addition table: %s", err)
	}
	q = "ALTER TABLE marctab.inc_add ADD CONSTRAINT marctab_add_pkey PRIMARY KEY (id);"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating primary key on addition table: %s", err)
	}
	var connw *pgx.Conn
	if connw, err = pgx.Connect(context.TODO(), dbc.ConnString); err != nil {
		return err
	}
	defer connw.Close(context.TODO())
	var tx pgx.Tx
	if tx, err = util.BeginTx(context.TODO(), connw); err != nil {
		return err
	}
	defer tx.Rollback(context.TODO())
	// transform
	q = filterQuery(srsRecords, srsMarc, srsMarcAttr, "marctab.inc_add")
	var rows pgx.Rows
	if rows, err = dbc.Conn.Query(context.TODO(), q); err != nil {
		return fmt.Errorf("selecting records to add: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, matchedID, instanceHRID, state, data *string
		var cksum string
		if err = rows.Scan(&id, &matchedID, &instanceHRID, &state, &data, &cksum); err != nil {
			return err
		}
		var instanceID string
		var mrecs []srs.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(id, matchedID, instanceHRID, state, data, printerr, verbose)
		if skip {
			continue
		}
		if _, err = uuid.EncodeUUID(instanceID); err != nil {
			printerr("id=%s: encoding instance_id %q: %v", *id, instanceID, err)
			instanceID = uuid.NilUUID
		}
		var m srs.Marc
		for _, m = range mrecs {
			q = "INSERT INTO " + tablefinal + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);"
			if _, err = tx.Exec(context.TODO(), q,
				id, m.Line, matchedID, instanceHRID, instanceID, m.Field, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content); err != nil {
				return fmt.Errorf("adding record: %v", err)
			}
		}
		// cksum
		if len(mrecs) != 0 {
			q = "INSERT INTO " + cksumTable + " VALUES ($1, $2);"
			if _, err = tx.Exec(context.TODO(), q, id, cksum); err != nil {
				return fmt.Errorf("adding cksum: %v", err)
			}
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS marctab.inc_add"); err != nil {
		return fmt.Errorf("dropping addition table: %s", err)
	}
	return nil
}

func updateDelete(dbc *util.DBC, srsRecords, tablefinal string, printerr func(string, ...any), verbose bool) error {
	var err error
	// find deleted data
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS marctab.inc_delete")
	var q = "CREATE UNLOGGED TABLE marctab.inc_delete AS SELECT c.id FROM " + srsRecords + " r RIGHT JOIN " + cksumTable + " c ON r.id::uuid = c.id WHERE r.id IS NULL;"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating deletion table: %s", err)
	}
	q = "ALTER TABLE marctab.inc_delete ADD CONSTRAINT marctab_delete_pkey PRIMARY KEY (id);"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating primary key on deletion table: %s", err)
	}
	if verbose {
		// show changes
		q = "SELECT id FROM marctab.inc_delete;"
		var rows pgx.Rows
		if rows, err = dbc.Conn.Query(context.TODO(), q); err != nil {
			return fmt.Errorf("reading deletion list: %s", err)
		}
		defer rows.Close()
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
	var connw *pgx.Conn
	if connw, err = pgx.Connect(context.TODO(), dbc.ConnString); err != nil {
		return fmt.Errorf("opening connection for writing: %v", err)
	}
	defer connw.Close(context.TODO())
	var tx pgx.Tx
	if tx, err = util.BeginTx(context.TODO(), connw); err != nil {
		return fmt.Errorf("opening transaction: %v", err)
	}
	defer tx.Rollback(context.TODO())
	// delete in finaltable
	q = "DELETE FROM " + tablefinal + " WHERE srs_id IN (SELECT id FROM marctab.inc_delete);"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("deleting records: %s", err)
	}
	// delete in cksum table
	q = "DELETE FROM " + cksumTable + " WHERE id IN (SELECT id FROM marctab.inc_delete);"
	if _, err = tx.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("deleting cksum: %s", err)
	}
	if err = tx.Commit(context.TODO()); err != nil {
		return fmt.Errorf("committing updates: %v", err)
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS marctab.inc_delete"); err != nil {
		return fmt.Errorf("dropping deletion table: %s", err)
	}
	return nil
}

func updateChange(dbc *util.DBC, srsRecords, srsMarc, srsMarcAttr, tablefinal string, printerr func(string, ...any), verbose bool) error {
	var err error
	// find changed data
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS marctab.inc_change")
	var q = "CREATE UNLOGGED TABLE marctab.inc_change AS SELECT r.id::uuid FROM " + srsRecords + " r JOIN " + cksumTable + " c ON r.id::uuid = c.id JOIN " + srsMarc + " m ON r.id = m.id WHERE " + util.MD5(srsMarcAttr) + " <> c.cksum;"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating change table: %s", err)
	}
	q = "ALTER TABLE marctab.inc_change ADD CONSTRAINT marctab_change_pkey PRIMARY KEY (id);"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("creating primary key on change table: %s", err)
	}
	// connR is used for queries concurrent with reading rows.
	var connR *pgx.Conn
	if connR, err = pgx.Connect(context.TODO(), dbc.ConnString); err != nil {
		return fmt.Errorf("opening connection for reading: %s", err)
	}
	defer connR.Close(context.TODO())
	// connW is used to write the changes.
	var connW *pgx.Conn
	if connW, err = pgx.Connect(context.TODO(), dbc.ConnString); err != nil {
		return fmt.Errorf("opening connection for writing: %s", err)
	}
	defer connW.Close(context.TODO())
	var tx pgx.Tx
	if tx, err = util.BeginTx(context.TODO(), connW); err != nil {
		return fmt.Errorf("opening transaction: %s", err)
	}
	defer tx.Rollback(context.TODO())
	// transform
	q = filterQuery(srsRecords, srsMarc, srsMarcAttr, "marctab.inc_change")
	var rows pgx.Rows
	if rows, err = dbc.Conn.Query(context.TODO(), q); err != nil {
		return fmt.Errorf("selecting records to change: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, matchedID, instanceHRID, state, data *string
		var cksum string
		if err = rows.Scan(&id, &matchedID, &instanceHRID, &state, &data, &cksum); err != nil {
			return fmt.Errorf("reading changes: %s", err)
		}
		var instanceID string
		var mrecs []srs.Marc
		var skip bool
		id, matchedID, instanceHRID, instanceID, mrecs, skip = util.Transform(id, matchedID, instanceHRID, state, data, printerr, verbose)
		if skip {
			continue
		}
		if _, err = uuid.EncodeUUID(instanceID); err != nil {
			printerr("id=%s: encoding instance_id %q: %v", *id, instanceID, err)
			instanceID = uuid.NilUUID
		}
		// check if there are existing rows in tablefinal
		var exist bool
		var i int64
		q = "SELECT 1 FROM " + tablefinal + " WHERE srs_id='" + *id + "' LIMIT 1"
		err = connR.QueryRow(context.TODO(), q).Scan(&i)
		switch {
		case err == pgx.ErrNoRows:
		case err != nil:
			return fmt.Errorf("checking for existing rows: %s", err)
		default:
			exist = true
		}
		// delete in tablefinal
		q = "DELETE FROM " + tablefinal + " WHERE srs_id = '" + *id + "';"
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("deleting record (change): %s", err)
		}
		// delete in cksum table
		q = "DELETE FROM " + cksumTable + " WHERE id = '" + *id + "';"
		if _, err = tx.Exec(context.TODO(), q); err != nil {
			return fmt.Errorf("deleting cksum (change): %s", err)
		}
		var m srs.Marc
		for _, m = range mrecs {
			q = "INSERT INTO " + tablefinal + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);"
			if _, err = tx.Exec(context.TODO(), q,
				id, m.Line, matchedID, instanceHRID, instanceID, m.Field, m.Ind1, m.Ind2, m.Ord, m.SF, m.Content); err != nil {
				return fmt.Errorf("rewriting record: %s", err)
			}
		}
		if verbose && exist && len(mrecs) == 0 {
			printerr("removing: id=%s", *id)
		}
		// cksum
		if len(mrecs) != 0 {
			q = "INSERT INTO " + cksumTable + " VALUES ($1, $2);"
			if _, err = tx.Exec(context.TODO(), q, id, cksum); err != nil {
				return fmt.Errorf("rewriting cksum: %s", err)
			}
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if err = tx.Commit(context.TODO()); err != nil {
		return err
	}
	if _, err = dbc.Conn.Exec(context.TODO(), "DROP TABLE IF EXISTS marctab.inc_change"); err != nil {
		return fmt.Errorf("dropping change table: %s", err)
	}
	return nil
}

func filterQuery(srsRecords, srsMarc, srsMarcAttr, filter string) string {
	return "" +
		"SELECT r.id::uuid, r.matched_id::uuid, r.external_hrid instance_hrid, r.state, m." + srsMarcAttr + ", " + util.MD5(srsMarcAttr) + " cksum " +
		"    FROM " + srsRecords + " r " +
		"        JOIN " + filter + " f ON r.id::uuid = f.id " +
		"        JOIN " + srsMarc + " m ON r.id = m.id;"
}
