package main

import (
	"os"

	"github.com/alecthomas/kingpin"

	"./hasher"
)

var (
	cwd, _  = os.Getwd()
	db      = kingpin.Flag("db", `Path to Music database to populate`).Short('d').Default(cwd + "/music.db").String()
	goprocs = kingpin.Flag("procs", "Use this number of processes to scrape data; number of concurrent filesystem readers to utilize").Short('p').Default("10").Int()

	assemble = kingpin.Command("assemble", "Assemble a Database by scraping a path")
	asroot   = assemble.Arg("PATH", "Root path to start walking looking for files").ExistingDir()

	analyze = kingpin.Command("analyze", "Analyze data to look for duplicates")

	dupNuke = kingpin.Command("dup-nuke", "Nuke (RM) located duplicated")

	moveKnown = kingpin.Command("move", "Move non-duplicatd files into another folder tree preserving <root>/<Artist>/<album>/<title> heirarchy")
	moveWhere = moveKnown.Arg("WHERE", "Move files to directory rooted here").ExistingDir()
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	which := kingpin.Parse()

	fdb := hasher.CreateFileDB(*db)
	defer fdb.Close()

	switch which {
	case assemble.FullCommand():
		fdb.PopulateDB(*asroot, *goprocs)
	case analyze.FullCommand():
		panicIf(fdb.Prune())
	case dupNuke.FullCommand():
		panicIf(fdb.DupNuker())
	case moveKnown.FullCommand():
		panicIf(fdb.RenameInto(*moveWhere))
	}
}
