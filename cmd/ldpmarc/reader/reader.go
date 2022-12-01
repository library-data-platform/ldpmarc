package reader

/*

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"
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

func ReadAll(txin pgx.Tx, srsRecords string, srsMarc string, srsMarcAttr string) <-chan Record {
	var ch = make(chan Record, 1000)
	var q = "SELECT r.id, r.matched_id, r.external_hrid instance_hrid, r.state, m." + srsMarcAttr + " FROM " + srsRecords + " r JOIN " + srsMarc + " m ON r.id = m.id;"
	var rows pgx.Rows
	var err error
	if rows, err = txin.Query(context.TODO(), q); err != nil {
		ch <- Record{Stop: true, Err: err}
		return ch
	}
	go scanAll(rows, ch)
	return ch
}

func scanAll(rows pgx.Rows, ch chan Record) {
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

*/
