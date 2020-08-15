package hasher

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // Import go-sqlite3 library
)

//CreateFileDB creates or opens an old BD
func CreateFileDB(path string) *FileDB {
	log.Printf("Creating / Opening %s\n", path)
	db, err := sqlx.Open("sqlite3", path)
	if err != nil {
		panic(err)
	}

	rtn := &FileDB{db: db, mutex: &sync.RWMutex{}}

	if err := rtn.createSchema(); err != nil {
		panic(err)
	}
	log.Printf("Created / Opened %s\n", path)
	return rtn
}

/*FileDB is a wrapper over a SQL database*/
type FileDB struct {
	db    *sqlx.DB
	mutex *sync.RWMutex
}

//Close closes the db
func (fdb *FileDB) Close() error {
	return fdb.db.Close()
}

/*Exec arbitrary SQL*/
func (fdb *FileDB) Exec(stmt string) (sql.Result, error) {
	fdb.mutex.Lock()
	defer fdb.mutex.Unlock()
	statement, err := fdb.db.Prepare(stmt) // Prepare SQL Statement
	if err != nil {
		return nil, err
	}
	return statement.Exec()
}

func (fdb *FileDB) createSchema() error {
	r := FileEntry{}
	schemas := []string{
		r.createStmt(),
		`CREATE TABLE IF NOT EXISTS rejects AS SELECT ' ' as reason, * FROM scanned_files LIMIT 0`,
		`CREATE TABLE IF NOT EXISTS duplicates AS SELECT *, ' ' as duplicate_of FROM scanned_files LIMIT 0`,
		`CREATE TABLE IF NOT EXISTS moved AS SELECT * FROM scanned_files LIMIT 0`,
	}
	for _, stmt := range schemas {
		if _, err := fdb.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

/*Insert a record*/
func (fdb *FileDB) Insert(record *FileEntry) error {
	fdb.mutex.Lock()
	defer fdb.mutex.Unlock()

	tx := fdb.db.MustBegin()
	stmt, err := tx.PrepareNamed(record.insertStmt())
	if err != nil {
		panic(err)
	}
	stmt.MustExec(record)
	return tx.Commit()
}

//MustExecMany simply blindly executes many sequential SQL strings
func (fdb *FileDB) MustExecMany(stmt []string) {
	log.Println("MustExecMany >")
	for idx, statement := range stmt {
		log.Printf("\t [%03d]: %s\n", idx, statement)
		if _, err := fdb.Exec(statement); err != nil {
			panic(err)
		}
	}
}

//WithDb allows you to directly access the database....
func (fdb *FileDB) WithDb(fxn func(*sqlx.DB)) {
	fdb.mutex.Lock()
	defer fdb.mutex.Unlock()
	fxn(fdb.db)
}

/*Keep adds */
func (fdb *FileDB) Keep(keep *FileEntry, dups Duplicates) error {
	dupStmt := fmt.Sprintf(`INSERT INTO duplicates SELECT *, %d as reason from scanned_files where id = ?`, keep.ID.Int64)
	tx := fdb.db.MustBegin()

	if len(dups) == 0 {
		return tx.Commit()
	}

	for _, id := range dups {
		tx.MustExec(dupStmt, id.ID.Int64)
	}
	return tx.Commit()
}

/*DupNuker removes all files located in the duplicate column.*/
func (fdb *FileDB) DupNuker() error {
	dstmt := `SELECT path FROM duplicates`
	tx := fdb.db.MustBegin()
	rows, err := tx.Queryx(dstmt)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		res := &FileEntry{}
		for _, fxn := range []func() error{
			func() error { return rows.StructScan(res) },
			res.Delete,
		} {
			if err := fxn(); err != nil {
				return err
			}
		}
	}
	return nil
}

/*RenameInto moves files from the source locations into <root>.

Generally, these get shoved in <root>/<artist>/<album>/<track> - <title>.<ext>
*/
func (fdb *FileDB) RenameInto(root string) error {
	rename := func() []int64 {
		ids := []int64{}

		fdb.mutex.Lock()
		defer fdb.mutex.Unlock()

		tx := fdb.db.MustBegin()
		cur, err := tx.Queryx(`SELECT * from scanned_files`)
		if err != nil {
			panic(err)
		}
		defer cur.Close()
		for cur.Next() {
			res := &FileEntry{}
			if err := cur.StructScan(res); err != nil {
				panic(err)
			}

			err := res.Rename(root)
			switch err.(type) {
			case nil:
				ids = append(ids, res.ID.Int64)
			case Skipped:
			default:
				panic(err)
			}
		}
		tx.Commit()
		return ids
	}

	markMoved := func(ids []int64) {
		fdb.mutex.Lock()
		defer fdb.mutex.Unlock()
		tx := fdb.db.MustBegin()
		for _, id := range ids {
			tx.MustExec(`INSERT INTO moved SELECT * FROM scanned_files WHERE id=?`, id)
		}
		tx.Commit()
	}
	ids := rename()
	markMoved(ids)

	fdb.MustExecMany([]string{`DELETE FROM scanned_files WHERE id IN (SELECT id FROM moved)`})

	return nil
}
