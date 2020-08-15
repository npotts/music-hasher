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
func PopulateDB(db string, rootPath string, goroutines int) error {
	fdb := CreateFileDB(db)
	defer fdb.Close()
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
	close(files)
	return nil
}
