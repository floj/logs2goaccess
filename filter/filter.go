package filter

import (
	"strings"

	"github.com/floj/logs2goaccess/goaccess"
)

type Filter func(*goaccess.Line) bool

func AddFilterIfNotEmpty(filters []Filter, v []string, fn func([]string) Filter) []Filter {
	if len(v) == 0 {
		return filters
	}
	return append(filters, fn(v))
}

func NewIncludeHostsPrefixFilter(prefixes []string) Filter {
	return func(l *goaccess.Line) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(l.VHost, p) {
				return true
			}
		}
		//fmt.Fprintf(os.Stderr, "excluding: VHost has no prefix in %v\n", prefixes)
		return false
	}
}

func NewExcludeClientsPrefixFilter(prefixes []string) Filter {
	return func(l *goaccess.Line) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(l.ClientIP, p) {
				//fmt.Fprintf(os.Stderr, "excluding: ClientIP has prefix %s\n", p)
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
				//fmt.Fprintf(os.Stderr, "excluding: URL has prefix %s\n", p)
				return false
			}
		}
		return true
	}
}
