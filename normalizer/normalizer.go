package normalizer

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/floj/logs2goaccess/goaccess"
)

type Normalizer func(*goaccess.Line) (*goaccess.Line, error)

func AddIfNotEmpty(normalizer []Normalizer, v []string, fn func([]string) (Normalizer, error)) ([]Normalizer, error) {
	if len(v) == 0 {
		return normalizer, nil
	}
	nn, err := fn(v)
	if err != nil {
		return nil, err
	}
	return append(normalizer, nn), nil
}

func NewURLNormalizer(s []string) (Normalizer, error) {
	if len(s) == 0 {
		return func(l *goaccess.Line) (*goaccess.Line, error) { return l, nil }, nil
	}

	match := []*regexp.Regexp{}
	replace := []string{}

	for _, e := range s {
		parts := strings.SplitN(e, "=>", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("'%s' does not contain a replacement, url normalizer must be <regexp>=><replacement>", e)
		}
		re, err := regexp.Compile(parts[0])
		if err != nil {
			return nil, fmt.Errorf("'%s' of '%s' if not a valid regexp: %w", parts[0], e, err)
		}
		match = append(match, re)
		replace = append(replace, parts[1])
	}
	return func(l *goaccess.Line) (*goaccess.Line, error) {
		for i := range match {
			l.URL = match[i].ReplaceAllString(l.URL, replace[i])
		}
		return l, nil
	}, nil
}
