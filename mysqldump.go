package mysqldump

import (
	"database/sql"
)

// Dumper represents a database.
type Dumper struct {
	db       *sql.DB
	filename string
}

/*
Creates a new dumper.

	db: Database that will be dumped (https://golang.org/pkg/database/sql/#DB).
	dir: Path to the directory where the dumps will be stored.
	format: Format to be used to name each dump file. Uses time.Time.Format (https://golang.org/pkg/time/#Time.Format). format appended with '.sql'.
*/
func Register(db *sql.DB, filename string) (*Dumper, error) {
	return &Dumper{
		db:       db,
	}, nil
}

// Closes the dumper.
// Will also close the database the dumper is connected to.
//
// Not required.
func (d *Dumper) Close() error {
	defer func() {
		d.db = nil
	}()
	return d.db.Close()
}

