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

/*resolveHashDups resolves duplicated by pooling all files with the same hash into a pool,
checking if the files in the pool are mostly the same, and if so, pickes on at random.

It pushes into 'originals` a single records, and pushes the dupicated pairs into
duplicates with pointers to the original record.

Once these dups have been 'handled', it prunes them from scanned_files */
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
		if keep := dupsWithSameHash.Resolve(); keep != nil {
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

/*Prune does some pre-defined sanity checks*/
func (fdb *FileDB) Prune() error {
	// Move obvious non-music files into rejected
	fdb.MustExecMany([]string{
		`INSERT INTO rejects SELECT 'Not Music File' as reason, * FROM scanned_files where lower(extension) not in ('.mp3', '.m4a', '.m4r')`,     // no non-music
		`DELETE FROM scanned_files WHERE id in (SELECT scanned_files.id from scanned_files INNER JOIN rejects ON scanned_files.id = rejects.id)`, //prune
	})

	for _, fxn := range []func() error{
		fdb.resolveHashDups,
	} {
		if err := fxn(); err != nil {
			panic(err)
		}
	}

	return nil
}
