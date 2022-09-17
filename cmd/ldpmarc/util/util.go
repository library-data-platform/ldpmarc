package util

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
)

func MD5(srsMarcAttr string) string {
	//return "md5(r::text || m::text)"
	return "md5(coalesce(r.external_hrid::text, '') || coalesce(r.matched_id::text, '') || coalesce(r.state::text, '') || coalesce(m." + srsMarcAttr + "::text, ''))"
}

func Transform(id, matchedID, instanceHRID, state, data *string, printerr func(string, ...interface{}), verbose bool) (*string, *string, *string, string, []srs.Marc, bool) {
	if id == nil {
		printerr(skipValue(id, data))
		return nil, nil, nil, "", nil, true
	}
	if strings.TrimSpace(*id) == "" {
		printerr(skipValue(id, data))
		return nil, nil, nil, "", nil, true
	}
	if data == nil {
		printerr(skipValue(id, data))
		return nil, nil, nil, "", nil, true
	}
	if strings.TrimSpace(*data) == "" {
		printerr(skipValue(id, data))
		return nil, nil, nil, "", nil, true
	}
	if matchedID == nil {
		s := ""
		matchedID = &s
	}
	if instanceHRID == nil {
		s := ""
		instanceHRID = &s
	}
	if state == nil {
		s := ""
		state = &s
	}
	var mrecs []srs.Marc
	var instanceID string
	var err error
	if mrecs, instanceID, err = srs.Transform(data, *state); err != nil {
		printerr(skipError(id, err))
		return nil, nil, nil, "", nil, true
	}
	if verbose && len(mrecs) != 0 {
		printerr("updating: id=%s", *id)
	}
	return id, matchedID, instanceHRID, instanceID, mrecs, false
}

func skipValue(id, data *string) string {
	return fmt.Sprintf("skipping record: %s", idData(id, data))
}

func skipError(id *string, err error) string {
	return fmt.Sprintf("skipping record: %s: %s", nullString(id), err)
}

func idData(id, data *string) string {
	return fmt.Sprintf("id=%s data=%s", nullString(id), nullString(data))
}

func nullString(s *string) string {
	if s != nil {
		return *s
	} else {
		return "(null)"
	}
}

func VacuumAnalyze(dbc *DBC, table string) error {
	var err error
	q := "VACUUM ANALYZE " + table + ";"
	if _, err = dbc.Conn.Exec(context.TODO(), q); err != nil {
		return fmt.Errorf("vacuuming table: %s: %s", table, err)
	}
	return nil
}

func GetAllFieldNames() []string {
	s := make([]string, 0)
	for i := 0; i <= 999; i++ {
		s = append(s, fmt.Sprintf("%03d", i))
	}
	return s
}

func IsTrgmAvailable(dbc *DBC) bool {
	if _, err := dbc.Conn.Exec(context.TODO(), "CREATE TEMP TABLE trgmtest (v varchar(1))"); err != nil {
		return false
	}
	if _, err := dbc.Conn.Exec(context.TODO(), "CREATE INDEX ON trgmtest USING GIN (v gin_trgm_ops)"); err != nil {
		return false
	}
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE trgmtest")
	return true
}

func IsLZ4Available(dbc *DBC) bool {
	if _, err := dbc.Conn.Exec(context.TODO(), "CREATE TEMP TABLE lz4test (v varchar(1) COMPRESSION lz4)"); err != nil {
		return false
	}
	_, _ = dbc.Conn.Exec(context.TODO(), "DROP TABLE lz4test")
	return true
}

func BeginTx(ctx context.Context, conn *pgx.Conn) (pgx.Tx, error) {
	var err error
	var tx pgx.Tx
	if tx, err = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: "read committed"}); err != nil {
		return nil, err
	}
	return tx, nil
}

type DBC struct {
	Conn       *pgx.Conn
	ConnString string
}
