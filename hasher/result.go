package hasher

import (
	"log"
	"os"
	"path/filepath"

	"github.com/cespare/xxhash"
	id3 "github.com/mikkyang/id3-go"
)

/*Result is some info about a file*/
type Result struct {
	ID        uint64
	Path      string
	Filename  string
	Extension string
	Title     string
	Album     string
	Artist    string
	Year      string
	TrackNo   int
	Size      int64
	XxHash    uint64
}

func (r *Result) id3Info(file *os.File) {
	file.Seek(0, 0)
	defer func() {
		if e := recover(); e != nil {
			log.Printf("Oops:%v  on %s ", e, file.Name())
		}
	}()
	if info, err := id3.Parse(file); err == nil {
		r.Title = info.Title()
		r.Album = info.Album()
		r.Artist = info.Artist()
		r.Year = info.Year()
	}
}

func (r *Result) xxhash(file *os.File) {
	file.Seek(0, 0)
	dig := xxhash.New()
	buff := make([]byte, 4096)
	for {
		n, err := file.Read(buff)
		dig.Write(buff[:n])
		if err != nil {
			r.XxHash = dig.Sum64()
			return
		}
	}
}

/*Parse reads from Path and returns some info about the file at Path*/
func Parse(path string) (*Result, error) {
	rst := &Result{
		ID:        0,
		Path:      path,
		Filename:  filepath.Base(path),
		Extension: filepath.Ext(path),
		TrackNo:   0,
	}

	file, err := os.Open(path)
	if err != nil {
		return rst, err
	}
	defer file.Close()

	if info, err := file.Stat(); err == nil {
		rst.Size = info.Size()
	}

	rst.id3Info(file)
	rst.xxhash(file)

	return rst, nil
}
