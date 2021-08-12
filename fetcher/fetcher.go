package fetcher

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
)

type Fetcher interface {
	io.Closer
	Next() (line string, lineRead bool, err error)
}

type FetcherImpl struct {
	locations []string
	current   io.ReadCloser
	s         *bufio.Scanner
}

func (f *FetcherImpl) Close() error {
	if f.current == nil {
		return nil
	}
	return f.Close()

}

var factories = map[string]func(string, string) (io.ReadCloser, error){
	"file:": func(prefix, s string) (io.ReadCloser, error) {
		f := strings.TrimPrefix(s, prefix)
		in, err := os.Open(f)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(f, ".gz") {
			return gzip.NewReader(in)
		}
		return in, nil

	},
}

func ForLocations(locations []string) (Fetcher, error) {
	return &FetcherImpl{
		locations: locations,
	}, nil
}

func (f *FetcherImpl) Next() (string, bool, error) {
	if len(f.locations) == 0 {
		return "", false, nil
	}
	if f.s == nil {
		r, err := open(f.locations[0])
		if err != nil {
			return "", false, err
		}
		f.current = r
		f.s = bufio.NewScanner(r)

	}
	if f.s.Scan() {
		return f.s.Text(), true, nil
	}
	if f.s.Err() != nil {
		return "", false, f.s.Err()
	}
	if err := f.current.Close(); err != nil {
		return "", false, err
	}
	f.current = nil
	f.s = nil
	f.locations = f.locations[1:]
	return f.Next()
}

func open(location string) (io.ReadCloser, error) {
	for p, fn := range factories {
		if strings.HasPrefix(location, p) {
			return fn(p, location)
		}
	}

	prefixes := []string{}
	for p := range factories {
		prefixes = append(prefixes, p)
	}
	return nil, fmt.Errorf("no fetcher for '%s' found, supported prefixes are %v", location, prefixes)
}
