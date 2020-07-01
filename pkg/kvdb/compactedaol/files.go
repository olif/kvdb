package compactedaol

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type filePathInfo struct {
	info     os.FileInfo
	filepath string
}

type byPath []filePathInfo

func (s byPath) Len() int {
	return len(s)
}

func (s byPath) Less(i, j int) bool {
	return s[i].filepath < s[j].filepath
}

func (s byPath) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// listFilesWithSuffix returns all files matching the suffix in descending
// order sorted by filepath
func listFilesWithSuffix(basepath, suffix string, desc bool) ([]filePathInfo, error) {
	files := []filePathInfo{}
	err := filepath.Walk(basepath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == suffix {
			files = append(files, filePathInfo{info, path})
		}
		return nil
	})

	if desc {
		sort.Sort(sort.Reverse(byPath(files)))
	} else {
		sort.Sort(byPath(files))
	}

	return files, err
}

func genFilename(suffix string) string {
	return fmt.Sprintf("%d%s", time.Now().UTC().UnixNano(), suffix)
}

func changeSuffix(filepath, oldSuffix, newSuffix string) string {
	return strings.Replace(filepath, oldSuffix, newSuffix, 1)
}
