package hasher

import "log"

/*Prune does some pre-defined sanity checks*/
func (fdb *FileDB) Prune() error {
	cleanUpStmts := []string{
		`INSERT INTO rejects SELECT 'Not Music File' as reason, * FROM scanned_files where lower(extension) not in ('.mp3', '.m4a', '.m4r')`,     // no non-music
		`DELETE FROM scanned_files WHERE id in (SELECT scanned_files.id from scanned_files INNER JOIN rejects ON scanned_files.id = rejects.id)`, //prune
		`DROP TABLE IF EXISTS duplicated_hashes`,
		`CREATE TABLE duplicated_hashes AS SELECT xxhash, count(*) AS count FROM scanned_files GROUP BY xxhash HAVING count(xxhash) > 1`,
	}
	for idx, statement := range cleanUpStmts {
		log.Printf("\t [%03d]: %s\n", idx, statement)
		if _, err := fdb.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}
