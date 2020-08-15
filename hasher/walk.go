package hasher

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"strings"
)

var ignore = []string{
	".DS_Store",
	"desktop.ini",
	".plist",
	"db_errlog",
	".strings",
	".pdf",
	".png",
	".gif",
}

func badApple(path string) bool {
	for _, s := range ignore {
		if strings.HasSuffix(path, s) {
			return true
		}
	}
	return false
}

/*PopulateDB creates a db*/
func (fdb *FileDB) PopulateDB(rootPath string, goroutines int) error {

	files := make(chan string, 16)
	wg := &sync.WaitGroup{}
	n := 0

	walkfunc := func(wpath string, info os.FileInfo, err error) error {
		if err != nil {
			panic(err)
		}
		if rootPath == wpath || info.IsDir() {
			log.Printf("Decending into %s\n", wpath)
			return nil
		}
		if !badApple(wpath) {
			n++
			wg.Add(1)
			go func() {
				files <- wpath
			}()
		}
		return nil
	}

	dbwriter := func() {
		for file := range files {
			fdb.Insert(NewFileEntry(file))
			log.Printf("✓: %s\n", file)
			wg.Done()
		}
	}

	for i := 0; i < goroutines; i++ {
		go dbwriter()
	}

	log.Printf("Starting Travese\n")
	filepath.Walk(rootPath, walkfunc)
	log.Printf("Awaiting Scan on %d files\n", n)
	wg.Wait()
	log.Println("Cleanup on isle", n)

	// Move obvious non-music files into rejected immediately
	fdb.MustExecMany([]string{
		`INSERT INTO rejects SELECT 'Not Music File' as reason, * FROM scanned_files where lower(extension) not in ('.mp3', '.m4a', '.m4r')`,              // no non-music
		`DELETE FROM scanned_files WHERE id in (SELECT scanned_files.id from scanned_files INNER JOIN rejects ON scanned_files.id = rejects.id)`,          // ... prune
		`CREATE TABLE IF NOT EXISTS missing_tags AS SELECT * FROM scanned_files WHERE title IS NULL OR album IS NULL OR  artist IS NULL;`,                 //Missing artists, title, etc - fix the tags first
		`DELETE FROM scanned_files WHERE id in (SELECT missing_tags.id from missing_tags INNER JOIN scanned_files ON scanned_files.id = missing_tags.id)`, // ... prune
	})
	close(files)
	return nil
}
