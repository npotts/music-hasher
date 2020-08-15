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
	r := Result{}
	schemas := []string{
		r.createStmt(),
		`CREATE TABLE IF NOT EXISTS rejects AS SELECT ' ' as reason, * FROM scanned_files LIMIT 0`,
		`CREATE TABLE IF NOT EXISTS duplicates AS SELECT *, ' ' as duplicate_of FROM scanned_files LIMIT 0`,
		`CREATE TABLE IF NOT EXISTS orignals AS SELECT * FROM scanned_files LIMIT 0`,
	}
	for _, stmt := range schemas {
		if _, err := fdb.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

/*Insert a record*/
func (fdb *FileDB) Insert(record *Result) error {
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
func (fdb *FileDB) Keep(keep *Result, dups Duplicates) error {
	keepStmt := `INSERT INTO orignals SELECT * from scanned_files where id = :id`
	dupStmt := fmt.Sprintf(`INSERT INTO duplicates SELECT *, %d as reason from scanned_files where id = ?`, keep.ID.Int64)
	fmt.Println(dupStmt)

	tx := fdb.db.MustBegin()
	stmt, err := tx.PrepareNamed(keepStmt)
	if err != nil {
		panic(err)
	}
	stmt.MustExec(keep)

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
		res := &Result{}
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
