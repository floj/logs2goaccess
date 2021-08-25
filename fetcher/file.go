package fetcher

import (
	"io"
	"os"

	"github.com/floj/logs2goaccess/filter"
)

func fileResolver(loc string) ([]string, error) {
	return []string{loc}, nil
}

func fileReader(loc string, fc filter.FilterConf) (io.ReadCloser, error) {
	in, err := os.Open(loc)
	if err != nil {
		return nil, err
	}
	return wrapGzipIfRequired(in, loc)
}
