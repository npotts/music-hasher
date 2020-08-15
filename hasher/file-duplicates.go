package hasher

import (
	"fmt"
	"log"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/xlab/tablewriter"
)

//Duplicates are a set FileEntrys that in general, are the same song
type Duplicates []*FileEntry

//String is a Stringer
func (d Duplicates) String() string {
	return strings.Join(d.choices("Identified Duplicate Set:"), "\n")
}

func (d Duplicates) choices(header string) []string {
	table := tablewriter.CreateTable()
	table.AddHeaders("ID", "Path", "Title", "Album", "Artist", "Track", "Size")
	for _, dup := range d {
		fmt.Println(dup)
		table.AddRow(dup.ID.Int64, dup.Path.String, dup.Title.String, dup.Album.String, dup.Artist.String, dup.TrackNo.Int64, dup.Size.Int64)
	}
	c := []string{header}
	for _, line := range strings.Split(table.Render(), "\n") {
		c = append(c, line)
	}
	return c
}

/*Uniques splits d into 2 seperate sets, and returns the unique ones.
* Set that are different as per comp
* Set that are the same as per comp

If len(d) < 2: returns (d, nil)
*/
func (d Duplicates) Uniques(comp FileEntryComparison) Duplicates {
	if len(d) < 2 {
		return d
	}

	unique := map[*FileEntry]bool{}
	similar := map[*FileEntry]bool{}

	for i := 0; i < len(d)-1; i++ {
		a := d[0]
		if val, ok := similar[a]; val && ok {
			continue //already the same as something else
		}
		unique[a] = true
		for _, b := range d[i+1:] {
			if comp(a, b) {
				similar[b] = true
			}
		}
	}

	f := func(m map[*FileEntry]bool) Duplicates {
		d := Duplicates{}
		for k := range m {
			d = append(d, k)
		}
		return d
	}
	//Same should only be one item
	return f(unique)
}

//OtherThan returns a copy of d, except for r.
func (d Duplicates) OtherThan(r *FileEntry) Duplicates {
	a := Duplicates{}
	for _, o := range d {
		if o != r {
			a = append(a, o)
		}
	}
	return a
}

/*Resolve Pickes the record to keep from a set.  If FileEntry is nil, it
indicates the user didnt want to make a choice, and should be discarded
*/
func (d Duplicates) Resolve(comp FileEntryComparison) *FileEntry {
	if len(d) < 1 {
		panic("Resolve only work when working with > 1 element")
	}

	chooser := func(some Duplicates) *FileEntry {
		choices := some.choices("Skip for now")
		prompt := promptui.Select{
			Size:         len(choices),
			Label:        "Select which to mark as 'keep'.  Selecting anything other than a row will skip",
			Items:        choices,
			CursorPos:    4,
			HideSelected: true,
		}
		i, _, err := prompt.Run()
		if err != nil {
			log.Fatalf("Prompt failed %v\n", err)
			panic(err)
		}
		//dont select the Table
		if i > 3 && i < len(choices)-2 {
			return d[i-4]
		}
		return nil
	}

	if comp != nil {
		diffs := d.Uniques(comp)
		if len(diffs) > 1 {
			// crap - we go more than 1 unique entry - We need the user to intervine
			return chooser(diffs)
		}
		return diffs[0]
	}
	return chooser(d)
}
