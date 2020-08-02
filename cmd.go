package main

import (
	"flag"
	"os"

	"./hasher"
)

// hasher.PopulateDB()
var (
	cwd, _  = os.Getwd()
	root    = flag.String("root", cwd, `Root path to start walking looking for files`)
	db      = flag.String("db", cwd+"/music.db", `Path to Music database to populate`)
	goprocs = flag.Int("readers", 10, `Number of concurrent filesystem readers to utilize`)
)

func main() {
	flag.Parse()
	hasher.PopulateDB(*db, *root, *goprocs)
}
