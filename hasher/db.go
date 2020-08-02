package hasher

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3" // Import go-sqlite3 library
)

//CreateFileDB creates or opens an old BD
func CreateFileDB(path string) *FileDB {
	log.Printf("Creating %s\n", path)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		panic(err)
	}

	rtn := &FileDB{db: db, mutex: &sync.RWMutex{}}

	if err := rtn.createSchema(); err != nil {
		panic(err)
	}
	log.Printf("Created %s\n", path)
	return rtn
}

/*FileDB is a wrapper over a SQL database*/
type FileDB struct {
	db    *sql.DB
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
	_, err := fdb.Exec(`CREATE TABLE IF NOT EXISTS files  (
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		path TEXT,
		filename TEXT,
		extension TEXT,
		title TEXT,
		album TEXT,
		artist TEXT,
		year INTEGER,
		track_no TEXT,
		size INTEGER,
		xxhash TEXT
	)`)
	return err
}

/*Insert a record*/
func (fdb *FileDB) Insert(record *Result) error {
	fdb.mutex.Lock()
	defer fdb.mutex.Unlock()
	stmt := `INSERT INTO files 
		(path, filename, extension, title, album, artist, year, track_no, size, xxhash)
	VALUES
		(?,?,?,?,?,?,?,?,?,?)`
	// stmt := `INSERT INTO files(code, name, program) VALUES (?, ?, ?)`
	statement, err := fdb.db.Prepare(stmt)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	_, err = statement.Exec(record.Path, record.Filename, record.Extension, record.Title, record.Album, record.Artist, record.Year, record.TrackNo, record.Size, fmt.Sprintf("%x", record.XxHash))
	if err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
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
