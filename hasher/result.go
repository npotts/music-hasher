package hasher

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/dhowden/tag"
)

func ns(s string) sql.NullString {
	if s != "" {
		return sql.NullString{String: s, Valid: true}
	}
	return sql.NullString{String: s, Valid: false}
}

/*Result is some info about a file*/
type Result struct {
	ID          sql.NullInt64  `db:"id"`
	Path        sql.NullString `db:"path"`
	Filename    sql.NullString `db:"filename"`
	Extension   sql.NullString `db:"extension"`
	Format      sql.NullString `db:"format"`
	FileType    sql.NullString `db:"file_type"`
	Title       sql.NullString `db:"title"`
	Album       sql.NullString `db:"album"`
	Artist      sql.NullString `db:"artist"`
	AlbumArtist sql.NullString `db:"album_artist"`
	Composer    sql.NullString `db:"composer"`
	Genre       sql.NullString `db:"genre"`
	Year        sql.NullInt64  `db:"year"`
	TrackNo     sql.NullInt64  `db:"track_no"`
	TrackTotal  sql.NullInt64  `db:"track_total"`
	DiskNo      sql.NullInt64  `db:"disk_no"`
	DiskTotal   sql.NullInt64  `db:"disk_total"`
	Comment     sql.NullString `db:"comment"`
	Size        sql.NullInt64  `db:"size"`
	XxHash      sql.NullString `db:"xxhash"`
}

func (*Result) createStmt() string {
	return `CREATE TABLE IF NOT EXISTS scanned_files (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, path TEXT, filename TEXT, extension TEXT, format TEXT, file_type TEXT, title TEXT, album TEXT, artist TEXT, album_artist TEXT, composer TEXT, genre TEXT, year INTEGER, track_no INTEGER, track_total INTEGER, disk_no INTEGER, disk_total INTEGER, comment TEXT, size INTEGER, xxhash TEXT)`
}

func (*Result) insertStmt() string {
	return `INSERT INTO scanned_files (path, filename, extension, format, file_type, title, album, artist, album_artist, composer, genre, year, track_no, track_total, disk_no, disk_total, comment, size, xxhash) VALUES (:path,:filename,:extension,:format,:file_type,:title,:album,:artist,:album_artist,:composer,:genre,:year,:track_no,:track_total,:disk_no,:disk_total,:comment,:size,:xxhash)`
}

func (r *Result) tagMetadata(file *os.File) {
	file.Seek(0, 0)
	if info, err := tag.ReadFrom(file); err == nil {
		r.Format = ns(string(info.Format()))
		r.FileType = ns(string(info.FileType()))
		r.Title = ns(info.Title())
		r.Album = ns(info.Album())
		r.Artist = ns(info.Artist())
		r.AlbumArtist = ns(info.AlbumArtist())
		r.Composer = ns(info.Composer())
		r.Genre = ns(info.Genre())
		r.Comment = ns(info.Comment())
		r.Year = sql.NullInt64{Int64: int64(info.Year()), Valid: true}
		a, b := info.Track()
		r.TrackNo = sql.NullInt64{Int64: int64(a), Valid: true}
		r.TrackTotal = sql.NullInt64{Int64: int64(b), Valid: true}
		a, b = info.Disc()
		r.DiskNo = sql.NullInt64{Int64: int64(a), Valid: true}
		r.DiskTotal = sql.NullInt64{Int64: int64(b), Valid: true}

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
			r.XxHash = sql.NullString{String: fmt.Sprintf("%d", dig.Sum64()), Valid: true}
			return
		}
	}
}

/*Parse reads from Path and returns some info about the file at Path*/
func Parse(path string) (*Result, error) {
	rst := &Result{
		Path:      ns(path),
		Filename:  ns(filepath.Base(path)),
		Extension: ns(strings.ToLower(filepath.Ext(path))),
	}

	file, err := os.Open(path)
	if err != nil {
		return rst, err
	}
	defer file.Close()

	if info, err := file.Stat(); err == nil {
		rst.Size = sql.NullInt64{Int64: info.Size(), Valid: true}
	}

	rst.tagMetadata(file)
	rst.xxhash(file)
	return rst, nil
}
