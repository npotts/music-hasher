package hasher

import (
	"log"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/xlab/tablewriter"
)

//Duplicates are a set Results that in general, are the same song
type Duplicates []*Result

//String is a Stringer
func (d Duplicates) String() string {
	return strings.Join(d.choices("Identified Duplicate Set:"), "\n")
}

func (d Duplicates) choices(header string) []string {
	table := tablewriter.CreateTable()
	table.AddHeaders("ID", "Path", "Title", "Album")
	for _, dup := range d {
		table.AddRow(dup.ID.Int64, dup.Path.String, dup.Title.String, dup.Album.String)
	}
	c := []string{header}
	for _, ch := range strings.Split(table.Render(), "\n") {
		c = append(c, ch)
	}
	return c
}

/*Resolve Pickes the record to keep from a set.  If Result is nil, it
indicates the user didnt want to make a choice, and should be discarded
*/
func (d Duplicates) Resolve() *Result {

	choices := d.choices("Skip for now")
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
