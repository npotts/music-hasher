package hasher

import (
	"log"

	"github.com/jmoiron/sqlx"
)

type hashcnt struct {
	XxHash string `db:"xxhash"`
	Count  int    `db:"count"`
}

func (h *hashcnt) Duplicates(db *sqlx.DB) Duplicates {
	recs := Duplicates{}
	db.Select(&recs, "SELECT * from scanned_files WHERE xxhash=$1", h.XxHash)
	if len(recs) != h.Count {
		log.Fatalf("Expected %d items with hash %q: but got %d instead. You need to look into this", h.Count, h.XxHash, len(recs))
	}
	return recs
}

type albAtrTitle struct {
	Title  string `db:"title"`
	Album  string `db:"album"`
	Artist string `db:"artist"`
}

func (a *albAtrTitle) Duplicates(db *sqlx.DB) Duplicates {
	recs := Duplicates{}
	db.Select(&recs, "SELECT * from scanned_files WHERE artist=$1 and album=$2 and title=$3", a.Artist, a.Album, a.Title)
	if len(recs) < 2 {
		log.Fatalf("Expected Duplicate items with title/album/artist %q/%q/%q You need to look into this", a.Title, a.Album, a.Artist)
	}
	return recs
}

/*resolveHashDups resolves duplicated by pooling all files with the same hash into a pool,
checking if the files in the pool are mostly the same, and if so, pickes on at random.

It pushes the dupicated pairs into duplicates with pointers to the original record.

Once these dups have been 'handled', it prunes them from scanned_files*/
func (fdb *FileDB) resolveHashDups() error {
	//build duplicated (hash, count) table
	fdb.MustExecMany([]string{
		`DROP TABLE IF EXISTS duplicated_hashes`,
		`CREATE TABLE duplicated_hashes AS SELECT xxhash, count(*) AS count FROM scanned_files GROUP BY xxhash HAVING count(xxhash) > 1`,
	})

	hashDups := []hashcnt{}

	fdb.mutex.Lock()
	fdb.db.Select(&hashDups, "SELECT * FROM duplicated_hashes")
	fdb.mutex.Unlock()

	for _, dup := range hashDups {
		dupsWithSameHash := dup.Duplicates(fdb.db)
		if keep := dupsWithSameHash.Resolve(SameExceptPath); keep != nil {
			toss := dupsWithSameHash.OtherThan(keep)
			if err := fdb.Keep(keep, toss); err != nil {
				return err
			}
		}
	}
	fdb.MustExecMany([]string{
		`DELETE FROM scanned_files WHERE xxhash in (SELECT xxhash from duplicated_hashes)`,
		`DROP TABLE IF EXISTS duplicated_hashes`,
	})
	return nil
}

func (fdb *FileDB) resolveSameArtistAlbumTitle() error {
	fdb.MustExecMany([]string{
		`DROP TABLE IF EXISTS duplicated_aat`,
		`CREATE TABLE duplicated_aat as 
			SELECT title, album, artist from (
				SELECT distinct title, album, artist from scanned_files group by artist, album, title having count(title) > 1 and album is not NULL and artist is not NULL
		)`,
	})

	artArtTitles := []albAtrTitle{}
	fdb.mutex.Lock()
	fdb.db.Select(&artArtTitles, `SELECT title, album, artist from duplicated_aat`)
	fdb.mutex.Unlock()

	for _, dup := range artArtTitles {
		dupsWithSameAAT := dup.Duplicates(fdb.db)
		if keep := dupsWithSameAAT.Resolve(nil); keep != nil {
			toss := dupsWithSameAAT.OtherThan(keep)
			if err := fdb.Keep(keep, toss); err != nil {
				return err
			}
		}
	}
	fdb.MustExecMany([]string{
		`DELETE FROM scanned_files WHERE id in (SELECT id from duplicates)`,
		`DROP TABLE IF EXISTS duplicated_aat`,
	})
	return nil
}

/*Prune does some pre-defined sanity checks*/
func (fdb *FileDB) Prune() error {
	//run through a set of cleanup functions
	for _, fxn := range []func() error{
		fdb.resolveHashDups,
		fdb.resolveSameArtistAlbumTitle,
	} {
		if err := fxn(); err != nil {
			panic(err)
		}
	}
	fdb.MustExecMany([]string{
		`DELETE FROM scanned_files WHERE id in (SELECT id from duplicates)`, // ... prune
	})

	return nil
}
