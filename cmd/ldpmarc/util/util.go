package util

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/reader"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
)

func MD5() string {
	//return "md5(r::text || m::text)"
	return "md5(coalesce(r.matched_id::text, '') || coalesce(r.instance_hrid::text, '') || coalesce(r.instance_id::text, '') || coalesce(m.data::text, ''))"
}

func Transform(r reader.Record, printerr func(string, ...interface{}), verbose bool) (string, string, string, string, []srs.Marc, bool) {
	if !r.ID.Valid {
		printerr(skipValue(r.ID, r.Data))
		return "", "", "", "", nil, true
	}
	var id string = r.ID.String
	if verbose {
		printerr("updating: id=%s", id)
	}
	if strings.TrimSpace(id) == "" {
		printerr(skipValue(r.ID, r.Data))
		return "", "", "", "", nil, true
	}
	if !r.Data.Valid {
		printerr(skipValue(r.ID, r.Data))
		return "", "", "", "", nil, true
	}
	var data string = r.Data.String
	if strings.TrimSpace(data) == "" {
		printerr(skipValue(r.ID, r.Data))
		return "", "", "", "", nil, true
	}
	var matchedID string = r.MatchedID.String
	if !r.MatchedID.Valid {
		matchedID = ""
	}
	var instanceHRID string = r.InstanceHRID.String
	if !r.InstanceHRID.Valid {
		instanceHRID = ""
	}
	var state string = r.State.String
	if !r.State.Valid {
		state = ""
	}
	var mrecs []srs.Marc
	var instanceID string
	var err error
	if mrecs, instanceID, err = srs.Transform(data, state); err != nil {
		printerr(skipError(r.ID, err))
		return "", "", "", "", nil, true
	}
	return id, matchedID, instanceHRID, instanceID, mrecs, false
}

func skipValue(idN, dataN sql.NullString) string {
	return fmt.Sprintf("skipping record: %s", idData(idN, dataN))
}

func skipError(idN sql.NullString, err error) string {
	return fmt.Sprintf("skipping record: %s: %s", nullString(idN), err)
}

func idData(idN, dataN sql.NullString) string {
	return fmt.Sprintf("id=%s data=%s", nullString(idN), nullString(dataN))
}

func nullString(s sql.NullString) string {
	if s.Valid {
		return s.String
	} else {
		return "(null)"
	}
}

func VacuumAnalyze(db *sql.DB, table string, noParallel bool) error {
	var err error
	// vacuum
	var q = "VACUUM "
	if noParallel {
		q = q + "(PARALLEL 0) "
	}
	q = q + table + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("vacuuming table: %s: %s", table, err)
	}
	// analyze
	q = "ANALYZE " + table + ";"
	if _, err = db.ExecContext(context.TODO(), q); err != nil {
		return fmt.Errorf("analyzing table: %s: %s", table, err)
	}
	return nil
}
