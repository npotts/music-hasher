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

/*FileEntry is some info about a file*/
type FileEntry struct {
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

/*NewFileEntry reads from Path and returns some info about the file at Path*/
func NewFileEntry(path string) *FileEntry {
	rst := &FileEntry{
		Path:      ns(path),
		Filename:  ns(filepath.Base(path)),
		Extension: ns(strings.ToLower(filepath.Ext(path))),
	}
	if p, err := filepath.Abs(path); err == nil {
		rst.Path = ns(p)
	}

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if info, err := file.Stat(); err == nil {
		rst.Size = sql.NullInt64{Int64: info.Size(), Valid: true}
	}

	rst.tagMetadata(file)
	rst.xxhash(file)
	return rst
}

func (*FileEntry) createStmt() string {
	return `CREATE TABLE IF NOT EXISTS scanned_files (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, path TEXT, filename TEXT, extension TEXT, format TEXT, file_type TEXT, title TEXT, album TEXT, artist TEXT, album_artist TEXT, composer TEXT, genre TEXT, year INTEGER, track_no INTEGER, track_total INTEGER, disk_no INTEGER, disk_total INTEGER, comment TEXT, size INTEGER, xxhash TEXT)`
}

func (*FileEntry) insertStmt() string {
	return `INSERT INTO scanned_files (path, filename, extension, format, file_type, title, album, artist, album_artist, composer, genre, year, track_no, track_total, disk_no, disk_total, comment, size, xxhash) VALUES (:path,:filename,:extension,:format,:file_type,:title,:album,:artist,:album_artist,:composer,:genre,:year,:track_no,:track_total,:disk_no,:disk_total,:comment,:size,:xxhash)`
}

func (r *FileEntry) tagMetadata(file *os.File) {
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

func (r *FileEntry) xxhash(file *os.File) {
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

//String is a stringer
func (r *FileEntry) String() string {
	s := "Music Source\n"
	s += fmt.Sprintf("\t- ID          :%d\n", r.ID.Int64)
	s += fmt.Sprintf("\t- Path        :%s\n", r.Path.String)
	s += fmt.Sprintf("\t- Filename    :%s\n", r.Filename.String)
	s += fmt.Sprintf("\t- Extension   :%s\n", r.Extension.String)
	s += fmt.Sprintf("\t- Format      :%s\n", r.Format.String)
	s += fmt.Sprintf("\t- FileType    :%s\n", r.FileType.String)
	s += fmt.Sprintf("\t- Title       :%s\n", r.Title.String)
	s += fmt.Sprintf("\t- Album       :%s\n", r.Album.String)
	s += fmt.Sprintf("\t- Artist      :%s\n", r.Artist.String)
	s += fmt.Sprintf("\t- AlbumArtist :%s\n", r.AlbumArtist.String)
	s += fmt.Sprintf("\t- Composer    :%s\n", r.Composer.String)
	s += fmt.Sprintf("\t- Genre       :%s\n", r.Genre.String)
	s += fmt.Sprintf("\t- Year        :%d\n", r.Year.Int64)
	s += fmt.Sprintf("\t- TrackNo     :%d\n", r.TrackNo.Int64)
	s += fmt.Sprintf("\t- TrackTotal  :%d\n", r.TrackTotal.Int64)
	s += fmt.Sprintf("\t- DiskNo      :%d\n", r.DiskNo.Int64)
	s += fmt.Sprintf("\t- DiskTotal   :%d\n", r.DiskTotal.Int64)
	s += fmt.Sprintf("\t- Comment     :%s\n", r.Comment.String)
	s += fmt.Sprintf("\t- Size        :%d\n", r.Size.Int64)
	s += fmt.Sprintf("\t- XxHash      :%s\n", r.XxHash.String)
	return s
}

//HasMetadata  if it has an title, artist,
func (r *FileEntry) HasMetadata() bool {
	return r.Title.Valid && r.Title.String != "" &&
		r.Album.Valid && r.Album.String != "" &&
		(r.Artist.Valid && r.Artist.String != "" || r.AlbumArtist.Valid && r.AlbumArtist.String != "") &&
		r.TrackNo.Valid && r.TrackNo.Int64 > 0
}

//Delete the file
func (r *FileEntry) Delete() error {
	if !r.Path.Valid {
		return nil
	}
	st, err := os.Stat(r.Path.String)
	if err == nil && st.Mode().IsRegular() {
		fmt.Printf("* bye-bye %s\n", r.Path.String)
		return os.Remove(r.Path.String)
	}
	fmt.Printf("* [Gone?] %s\n", r.Path.String)
	return nil
}

//Skipped is a special error indicating that this file record info was skipped / not moved
type Skipped error

func fromE(err string, args ...interface{}) Skipped {
	return Skipped(fmt.Errorf(err, args...))
}

//ValidFormat returns true if the format and file type are ok
func (r *FileEntry) ValidFormat() bool {
	if !r.Format.Valid {
		return false
	}
	switch r.Format.String {
	case string(tag.UnknownFormat):
		return false
	case string(tag.MP4):
		return true
	default:
		return r.FileType.Valid && r.FileType.String != string(tag.UnknownFileType)
	}
}

//NewName Suggests a new name
func (r *FileEntry) NewName() string {
	un := func(p sql.NullString) string {
		if p.String == "" {
			return "Unknown"
		}
		return strings.ReplaceAll(p.String, "\\", "\\\\")
	}
	ui := func(p sql.NullInt64) string {
		if p.Int64 == 0 {
			return "00"
		}
		return fmt.Sprintf("%02d", p.Int64)
	}
	if r.AlbumArtist.Valid && r.AlbumArtist.String != "" {
		return filepath.Join(un(r.Artist), un(r.AlbumArtist), ui(r.TrackNo)+" "+un(r.Title)+r.Extension.String)
	}
	return filepath.Join(un(r.Artist), un(r.Album), ui(r.TrackNo)+" "+un(r.Title)+r.Extension.String)
}

/*Rename moves the file at `path` to a new path determined by:
 * <Root>/<Artist>/<Album>/<TrackNo>. - <Track>.<ext>
It refuses to move items that:
* Do not currently exist on the FS
* Do not have scanned data type (Format or FileType not a known value)
* Artist, Album, or Title are "Null".

It also:
* Replaces all empty values for Artist, Albums are replaced with "Unknown"
* Will not replace files unless asking.

*/
func (r *FileEntry) Rename(root string) error {
	//MP4 doesnt have a FileType.
	if !r.ValidFormat() {
		return fromE("Invalid file - unknown type or format")
	}
	if !r.Album.Valid || !r.Artist.Valid || !r.Title.Valid {
		return fromE("Artist, Album and Title cannot be NULL values")
	}
	if stat, err := os.Stat(r.Path.String); err != nil || !stat.Mode().IsRegular() {
		return fromE("Appears to not exist on file s.  ystem: %v", err)
	}

	newPath := filepath.Join(root, r.NewName())
	if st, er := os.Stat(newPath); er == nil && st.Mode().IsRegular() {
		return fromE("Remote Path Exists!!!")
	}

	mktree := func(path string) {
		parent, _ := filepath.Split(path)
		if err := os.MkdirAll(parent, os.ModePerm); err != nil {
			panic(err)
		}
	}
	mktree(newPath)
	return os.Rename(r.Path.String, newPath)
}

//A FileEntryComparison returns True if the two results are similar enough by some mechanism
type FileEntryComparison func(*FileEntry, *FileEntry) bool

/*SameExceptPath returns True if everything except the following are identical:
* ID
* Path
* Filename
 */
func SameExceptPath(r, o *FileEntry) (same bool) {
	// defer func() { fmt.Println("r, o are same: ", same) }()
	if r == nil || o == nil {
		panic("Cannot perform comparison with nil FileEntrys")
	}
	return r.HasMetadata() && o.HasMetadata() &&
		r.Extension.String == o.Extension.String && r.Extension.Valid == o.Extension.Valid &&
		r.Format.String == o.Format.String && r.Format.Valid == o.Format.Valid &&
		r.FileType.String == o.FileType.String && r.FileType.Valid == o.FileType.Valid &&
		r.Title.String == o.Title.String && r.Title.Valid == o.Title.Valid &&
		r.Album.String == o.Album.String && r.Album.Valid == o.Album.Valid &&
		r.Artist.String == o.Artist.String && r.Artist.Valid == o.Artist.Valid &&
		r.AlbumArtist.String == o.AlbumArtist.String && r.AlbumArtist.Valid == o.AlbumArtist.Valid &&
		r.Composer.String == o.Composer.String && r.Composer.Valid == o.Composer.Valid &&
		r.Genre.String == o.Genre.String && r.Genre.Valid == o.Genre.Valid &&
		r.Year.Int64 == o.Year.Int64 && r.Year.Valid == o.Year.Valid &&
		r.TrackNo.Int64 == o.TrackNo.Int64 && r.TrackNo.Valid == o.TrackNo.Valid &&
		r.TrackTotal.Int64 == o.TrackTotal.Int64 && r.TrackTotal.Valid == o.TrackTotal.Valid &&
		r.DiskNo.Int64 == o.DiskNo.Int64 && r.DiskNo.Valid == o.DiskNo.Valid &&
		r.DiskTotal.Int64 == o.DiskTotal.Int64 && r.DiskTotal.Valid == o.DiskTotal.Valid &&
		r.Comment.String == o.Comment.String && r.Comment.Valid == o.Comment.Valid &&
		r.Size.Int64 == o.Size.Int64 && r.Size.Valid == o.Size.Valid &&
		r.XxHash.String == o.XxHash.String && r.XxHash.Valid == o.XxHash.Valid
}
