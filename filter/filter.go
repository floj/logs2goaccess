package filter

import (
	"strings"

	"github.com/floj/logs2goaccess/goaccess"
)

type Filter func(*goaccess.Line) bool

func AddFilterIfNotEmpty(filters []Filter, v string, fn func([]string) Filter) []Filter {
	if v == "" {
		return filters
	}
	filter := fn(strings.Split(v, ","))
	return append(filters, filter)
}

func NewIncludeHostsPrefixFilter(prefixes []string) Filter {
	return func(l *goaccess.Line) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(l.VHost, p) {
				return true
			}
		}
		return false
	}
}

func NewExcludeClientsPrefixFilter(prefixes []string) Filter {
	return func(l *goaccess.Line) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(l.ClientIP, p) {
				return false
			}
		}
		return true
	}
}

func NewExcludeURLsPrefixFilter(prefixes []string) Filter {
	return func(l *goaccess.Line) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(l.URL, p) {
				return false
			}
		}
		return true
	}
}
