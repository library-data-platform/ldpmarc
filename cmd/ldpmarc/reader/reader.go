package reader

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
)

type Reader struct {
	pos          int
	records      []srs.Marc
	id           string
	matchedID    string
	instanceHRID string
	instanceID   string
	rows         *sql.Rows
	verbose      bool
}

func NewReader(txin *sql.Tx, srsRecords string, srsMarc string, verbose bool, limit int) (*Reader, int64, error) {
	var err error
	// Read number of input records
	var total int64
	if total, err = selectCount(txin, srsRecords); err != nil {
		return nil, 0, err
	}
	// Set up Reader
	var r = &Reader{}
	var lim string
	if limit != -1 {
		lim = " LIMIT " + strconv.Itoa(limit)
		if int64(limit) < total {
			total = int64(limit)
		}
	}
	var q = "SELECT r.id, r.matched_id, r.instance_hrid, r.state, m.data FROM " + srsRecords + " r JOIN " + srsMarc + " m ON r.id = m.id ORDER BY r.id" + lim + ";"
	if r.rows, err = txin.QueryContext(context.TODO(), q); err != nil {
		return nil, 0, err
	}
	r.records = []srs.Marc{}
	r.verbose = verbose
	return r, total, nil
}

func (r *Reader) Close() {
	r.rows.Close()
}

func (r *Reader) Next(printerr func(string, ...interface{})) (bool, error) {
	var err error
	for {
		if r.pos < len(r.records) {
			return true, nil
		}
		if !r.rows.Next() {
			err = r.rows.Err()
			if err != nil {
				return false, err
			}
			return false, nil
		}
		var idN, matchedIDN, instanceHRIDN, stateN, dataN sql.NullString
		if err = r.rows.Scan(&idN, &matchedIDN, &instanceHRIDN, &stateN, &dataN); err != nil {
			return false, err
		}
		err = r.rows.Err()
		if err != nil {
			return false, err
		}
		if !idN.Valid {
			printerr(skipValue(idN, dataN))
			continue
		}
		var id string = idN.String
		if r.verbose {
			printerr("verbose: read id=%s", id)
		}
		if strings.TrimSpace(id) == "" {
			printerr(skipValue(idN, dataN))
			continue
		}
		if !dataN.Valid {
			printerr(skipValue(idN, dataN))
			continue
		}
		var data string = dataN.String
		if strings.TrimSpace(data) == "" {
			printerr(skipValue(idN, dataN))
			continue
		}
		var matchedID string = matchedIDN.String
		if !matchedIDN.Valid {
			matchedID = ""
		}
		var instanceHRID string = instanceHRIDN.String
		if !instanceHRIDN.Valid {
			instanceHRID = ""
		}
		var state string = stateN.String
		if !stateN.Valid {
			state = ""
		}
		var instanceID string
		if r.records, instanceID, err = srs.Transform(data, state); err != nil {
			printerr(skipError(idN, err))
			continue
		}
		r.pos = 0
		r.id = id
		r.matchedID = matchedID
		r.instanceHRID = instanceHRID
		r.instanceID = instanceID
	}
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

func (r *Reader) Values() (string, string, string, string, *srs.Marc) {
	var m srs.Marc = r.records[r.pos]
	r.pos++
	return r.id,
		r.matchedID,
		r.instanceHRID,
		r.instanceID,
		&srs.Marc{
			Line:    m.Line,
			BibID:   m.BibID,
			Field:   m.Field,
			Ind1:    m.Ind1,
			Ind2:    m.Ind2,
			Ord:     m.Ord,
			SF:      m.SF,
			Content: m.Content,
		}
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
