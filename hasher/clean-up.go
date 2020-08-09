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

func (fdb *FileDB) hashDuplicates() (dups []hashcnt) {
	//build duplicated (hash, count) table
	fdb.MustExecMany([]string{
		`DROP TABLE IF EXISTS duplicated_hashes`,
		`CREATE TABLE duplicated_hashes AS SELECT xxhash, count(*) AS count FROM scanned_files GROUP BY xxhash HAVING count(xxhash) > 1`,
	})

	fdb.mutex.Lock()
	defer fdb.mutex.Unlock()

	fdb.db.Select(&dups, "SELECT * FROM duplicated_hashes")
	return dups
}

/*Prune does some pre-defined sanity checks*/
func (fdb *FileDB) Prune() error {
	// Move obvious non-music files into rejected
	fdb.MustExecMany([]string{
		`INSERT INTO rejects SELECT 'Not Music File' as reason, * FROM scanned_files where lower(extension) not in ('.mp3', '.m4a', '.m4r')`,     // no non-music
		`DELETE FROM scanned_files WHERE id in (SELECT scanned_files.id from scanned_files INNER JOIN rejects ON scanned_files.id = rejects.id)`, //prune
	})

	for _, hashset := range fdb.hashDuplicates() {
		fdb.mutex.Lock()
		dups := hashset.Duplicates(fdb.db)
		fdb.mutex.Unlock()
		keep := dups.Resolve()
		log.Printf("Want to keep %v\n", keep)
	}

	return nil
}
