package reader

import (
	"context"
	"database/sql"
)

type Record struct {
	Stop         bool
	Err          error
	ID           sql.NullString
	MatchedID    sql.NullString
	InstanceHRID sql.NullString
	State        sql.NullString
	Data         sql.NullString
}

func ReadAll(txin *sql.Tx, srsRecords string, srsMarc string, verbose bool) <-chan Record {
	var ch = make(chan Record, 1000)
	var q = "SELECT r.id, r.matched_id, r.instance_hrid, r.state, m.data FROM " + srsRecords + " r JOIN " + srsMarc + " m ON r.id = m.id;"
	var rows *sql.Rows
	var err error
	if rows, err = txin.QueryContext(context.TODO(), q); err != nil {
		ch <- Record{Stop: true, Err: err}
		return ch
	}
	go scanAll(rows, ch)
	return ch
}

func scanAll(rows *sql.Rows, ch chan Record) {
	var err error
	for {
		if !rows.Next() {
			if err = rows.Err(); err != nil {
				ch <- Record{Stop: true, Err: err}
				return
			}
			rows.Close()
			ch <- Record{Stop: true, Err: nil}
			return
		}
		var id, matchedID, instanceHRID, state, data sql.NullString
		if err = rows.Scan(&id, &matchedID, &instanceHRID, &state, &data); err != nil {
			ch <- Record{Stop: true, Err: err}
			return
		}
		if err = rows.Err(); err != nil { // this check may be unnecessary
			ch <- Record{Stop: true, Err: err}
			return
		}
		ch <- Record{
			Stop:         false,
			Err:          nil,
			ID:           id,
			MatchedID:    matchedID,
			InstanceHRID: instanceHRID,
			State:        state,
			Data:         data,
		}
	}
}
