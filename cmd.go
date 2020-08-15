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
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	which := kingpin.Parse()
	switch which {
	case assemble.FullCommand():
		hasher.PopulateDB(*db, *asroot, *goprocs)
	case analyze.FullCommand():
		fdb := hasher.CreateFileDB(*db)
		defer fdb.Close()
		panicIf(fdb.Prune())
	case dupNuke.FullCommand():
		fdb := hasher.CreateFileDB(*db)
		panicIf(fdb.DupNuker())
		defer fdb.Close()
	}
}
