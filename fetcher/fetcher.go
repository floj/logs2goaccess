package fetcher

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/floj/logs2goaccess/filter"
)

type Fetcher interface {
	io.Closer
	Next() (line string, lineRead bool, err error)
}

type FetcherImpl struct {
	locations []string
	fc        filter.FilterConf

	current io.ReadCloser
	s       *bufio.Scanner
}

func (f *FetcherImpl) Close() error {
	if f.current == nil {
		return nil
	}
	return f.Close()
}

type locationResolver func(s string) ([]string, error)

var factories = map[string]func(string, filter.FilterConf) (io.ReadCloser, error){
	"file:":   fileReader,
	"s3:":     s3Reader,
	"cwlogs:": cwLogsReader,
}

var locationResolvers = map[string]func(s string) ([]string, error){
	"file:":  fileResolver,
	"s3:":    s3LocationResolver,
	"cwlogs": cwLogsResolver,
}

func findMatchers(loc string) (string, matcher, error) {
	parts := strings.Split(loc, "|")
	if len(parts) == 1 {
		return loc, func(s string) bool { return true }, nil
	}
	matchers := []matcher{}
	for _, p := range parts[1:] {
		m, err := newMatcher(p)
		if err != nil {
			return "", nil, err
		}
		matchers = append(matchers, m)
	}
	return parts[0], func(s string) bool {
		for _, m := range matchers {
			if !m(s) {
				return false
			}
		}
		return true
	}, nil
}

func resolverFor(loc string) (locationResolver, bool) {
	for prefix, resolver := range locationResolvers {
		if strings.HasPrefix(loc, prefix) {
			return resolver, true
		}
	}
	return nil, false
}

func ForLocations(locations []string, fc filter.FilterConf) (Fetcher, error) {
	locs := []string{}
	for _, loc := range locations {
		resolver, set := resolverFor(loc)
		if !set {
			validResolvers := []string{}
			for k := range locationResolvers {
				validResolvers = append(validResolvers, k)
			}
			return nil, fmt.Errorf("no location resolver for '%s' present, known resolvers: %v", loc, validResolvers)
		}
		resolved, err := resolver(loc)
		if err != nil {
			return nil, err
		}
		locs = append(locs, resolved...)
	}
	return &FetcherImpl{
		locations: locs,
		fc:        fc,
	}, nil
}

func (f *FetcherImpl) Next() (string, bool, error) {
	if len(f.locations) == 0 {
		return "", false, nil
	}
	if f.s == nil {
		r, err := open(f.locations[0], f.fc)
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

func wrapGzipIfRequired(in io.ReadCloser, name string) (io.ReadCloser, error) {
	ext := filepath.Ext(name)
	if strings.ToLower(ext) == ".gz" {
		return gzip.NewReader(in)
	}
	return in, nil
}

func open(location string, fc filter.FilterConf) (io.ReadCloser, error) {
	fmt.Fprintf(os.Stderr, "opening %s\n", location)
	for p, fn := range factories {
		if strings.HasPrefix(location, p) {
			s := strings.TrimPrefix(location, p)
			return fn(s, fc)
		}
	}

	prefixes := []string{}
	for p := range factories {
		prefixes = append(prefixes, p)
	}
	return nil, fmt.Errorf("no fetcher for '%s' found, supported prefixes are %v", location, prefixes)
}

type matcher func(string) bool

func newMatcher(def string) (matcher, error) {
	if strings.HasPrefix(def, "rexexp:") {
		pattern := strings.TrimPrefix(def, "regexp:")
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		return re.MatchString, nil
	}

	if strings.HasPrefix(def, "suffix:") {
		suf := strings.TrimPrefix(def, "suffix:")
		return func(s string) bool { return strings.HasSuffix(s, suf) }, nil
	}

	return nil, fmt.Errorf("unknown matcher definition: '%s'", def)
}
