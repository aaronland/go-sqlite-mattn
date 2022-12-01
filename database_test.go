package mattn

import (
	"context"
	"github.com/aaronland/go-sqlite/v2"
	"testing"
)

func TestNewDatabase(t *testing.T) {

	ctx := context.Background()

	uri := "mattn://mem"

	db, err := sqlite.NewDatabase(ctx, uri)

	if err != nil {
		t.Fatalf("Failed to create database for '%s', %v", uri, err)
	}

	dsn := db.DSN(ctx)

	if dsn != "file::memory:?mode=memory&cache=shared" {
		t.Fatalf("Invalid DSN string '%s'", dsn)
	}
}
