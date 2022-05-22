package writer

/*

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
)

type Record struct {
	Close        bool
	Stop         bool
	Err          error
	SRSID        string
	Line         int64
	MatchedID    string
	InstanceHRID string
	InstanceID   string
	Field        string
	Ind1         string
	Ind2         string
	Ord          int64
	SF           string
	Content      string
}

func WriteAll(txout pgx.Tx, schema string, table string) (chan Record, <-chan error) {
	var ch = make(chan Record, 1000)
	var closed = make(chan error)
	var stmt *sql.Stmt
	var err error
	if stmt, err = txout.PrepareContext(context.TODO(), pq.CopyInSchema(schema, table,
		"srs_id", "line", "matched_id", "instance_hrid", "instance_id", "field", "ind1", "ind2", "ord", "sf", "content")); err != nil {
		ch <- Record{Stop: true, Err: err}
		return ch, closed
	}
	go copyAll(stmt, ch, closed)
	return ch, closed
}

func copyAll(stmt *sql.Stmt, ch chan Record, closed chan error) {
	// var err error
	for {
		var r Record = <-ch
		if r.Close {
			if _, err = stmt.ExecContext(context.TODO()); err != nil {
				closed <- err
				return
			}
			if err = stmt.Close(); err != nil {
				closed <- err
				return
			}
			closed <- nil
			return
		}
		if _, err = stmt.ExecContext(context.TODO(), r.SRSID, r.Line, r.MatchedID, r.InstanceHRID, r.InstanceID, r.Field, r.Ind1, r.Ind2, r.Ord, r.SF, r.Content); err != nil {
			ch <- Record{Stop: true, Err: err}
			return
		}
	}
}

*/
