package hasher

import (
	"database/sql"
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

// /*func main() {
// 	os.Remove("sqlite-database.db") // I delete the file to avoid duplicated records. SQLite is a file based database.

// 	log.Println("Creating sqlite-database.db...")
// 	file, err := os.Create("sqlite-database.db") // Create SQLite file
// 	if err != nil {
// 		log.Fatal(err.Error())
// 	}
// 	file.Close()
// 	log.Println("sqlite-database.db created")

// 	sqliteDatabase, _ := sql.Open("sqlite3", "./sqlite-database.db") // Open the created SQLite File
// 	defer sqliteDatabase.Close() // Defer Closing the database
// 	createTable(sqliteDatabase) // Create Database Tables

//         // INSERT RECORDS
// 	insertStudent(sqliteDatabase, "0001", "Liana Kim", "Bachelor")
// 	insertStudent(sqliteDatabase, "0002", "Glen Rangel", "Bachelor")
// 	insertStudent(sqliteDatabase, "0003", "Martin Martins", "Master")
// 	insertStudent(sqliteDatabase, "0004", "Alayna Armitage", "PHD")
// 	insertStudent(sqliteDatabase, "0005", "Marni Benson", "Bachelor")
// 	insertStudent(sqliteDatabase, "0006", "Derrick Griffiths", "Master")
// 	insertStudent(sqliteDatabase, "0007", "Leigh Daly", "Bachelor")
// 	insertStudent(sqliteDatabase, "0008", "Marni Benson", "PHD")
// 	insertStudent(sqliteDatabase, "0009", "Klay Correa", "Bachelor")

//         // DISPLAY INSERTED RECORDS
// 	displayStudents(sqliteDatabase)
// }*/

// func createTable(db *sql.DB) {
// 	createStudentTableSQL := `CREATE TABLE student (
// 		"idStudent" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
// 		"code" TEXT,
// 		"name" TEXT,
// 		"program" TEXT
// 	  );` // SQL Statement for Create Table

// 	log.Println("Create student table...")
// 	statement, err := db.Prepare(createStudentTableSQL) // Prepare SQL Statement
// 	if err != nil {
// 		log.Fatal(err.Error())
// 	}
// 	statement.Exec() // Execute SQL Statements
// 	log.Println("student table created")
// }

// // We are passing db reference connection from main to our method with other parameters
// func insertStudent(db *sql.DB, code string, name string, program string) {
// 	log.Println("Inserting student record ...")
// 	insertStudentSQL := `INSERT INTO student(code, name, program) VALUES (?, ?, ?)`
// 	statement, err := db.Prepare(insertStudentSQL) // Prepare statement. This is good to avoid SQL injections
// 	if err != nil {
// 		log.Fatalln(err.Error())
// 	}
// 	_, err = statement.Exec(code, name, program)
// 	if err != nil {
// 		log.Fatalln(err.Error())
// 	}
// }

// func displayStudents(db *sql.DB) {
// 	row, err := db.Query("SELECT * FROM student ORDER BY name")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer row.Close()
// 	for row.Next() { // Iterate and fetch the records from result cursor
// 		var id int
// 		var code string
// 		var name string
// 		var program string
// 		row.Scan(&id, &code, &name, &program)
// 		log.Println("Student: ", code, " ", name, " ", program)
// 	}
// }
