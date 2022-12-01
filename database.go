package mattn

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aaronland/go-sqlite/v2"
	_ "github.com/mattn/go-sqlite3"
	"net/url"
	"sync"
)

const SQLITE_SCHEME string = "mattn"
const SQLITE_DRIVER string = "sqlite3"

type MattnDatabase struct {
	sqlite.Database
	conn *sql.DB
	dsn  string
	mu   *sync.Mutex
}

func init() {
	ctx := context.Background()
	sqlite.RegisterDatabase(ctx, SQLITE_SCHEME, NewMattnDatabase)
}

func NewMattnDatabase(ctx context.Context, db_uri string) (sqlite.Database, error) {

	u, err := url.Parse(db_uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	host := u.Host
	path := u.Path
	q := u.RawQuery

	// make this a generic function in aaronland/go-sqlite ?

	/*

		if !strings.HasPrefix(dsn, "file:") {

			// because this and this:

			if dsn == ":memory:" {

				// https://github.com/mattn/go-sqlite3#faq
				// https://github.com/mattn/go-sqlite3/issues/204

				dsn = "file::memory:?mode=memory&cache=shared"

			} else if strings.HasPrefix(dsn, "vfs:") {

				// see also: https://github.com/aaronland/go-sqlite-vfs
				// pass

			} else {

				// https://github.com/mattn/go-sqlite3/issues/39
				dsn = fmt.Sprintf("file:%s?cache=shared&mode=rwc", dsn)

			}
		}
	*/

	var dsn string

	if host == "mem" {
		dsn = "file::memory:?mode=memory&cache=shared"
	} else {
		dsn = fmt.Sprintf("file:%s?cache=shared&mode=rwc", path)
	}

	if q != "" {
		dsn = fmt.Sprintf("%s?%s", dsn, q)
	}

	conn, err := sql.Open(SQLITE_DRIVER, dsn)

	if err != nil {
		return nil, fmt.Errorf("Failed to open database connection, %w", err)
	}

	mu := new(sync.Mutex)

	db := MattnDatabase{
		conn: conn,
		dsn:  dsn,
		mu:   mu,
	}

	return &db, nil
}

func (db *MattnDatabase) Lock(ctx context.Context) error {
	db.mu.Lock()
	return nil
}

func (db *MattnDatabase) Unlock(ctx context.Context) error {
	db.mu.Unlock()
	return nil
}

func (db *MattnDatabase) Conn(ctx context.Context) (*sql.DB, error) {
	return db.conn, nil
}

func (db *MattnDatabase) Close(ctx context.Context) error {
	return db.conn.Close()
}

func (db *MattnDatabase) DSN(ctx context.Context) string {
	return db.dsn
}
