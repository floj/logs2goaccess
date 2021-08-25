package filter

import (
	"strings"
	"time"

	"github.com/floj/logs2goaccess/goaccess"
)

type FilterConf struct {
	DateAfter           *time.Time
	DateBefore          *time.Time
	IncludeHostPrefix   []string
	ExcludeClientPrefix []string
	ExcludeURLPrefix    []string
}

func (c *FilterConf) Build() (Filter, error) {
	filters := []Filter{}
	filters = AddIfNotEmpty(filters, c.IncludeHostPrefix, NewIncludeHostsPrefixFilter)
	filters = AddIfNotEmpty(filters, c.ExcludeClientPrefix, NewExcludeClientsPrefixFilter)
	filters = AddIfNotEmpty(filters, c.ExcludeURLPrefix, NewExcludeURLsPrefixFilter)
	if c.DateAfter != nil {
		filters = append(filters, NewDateAfterFilter(*c.DateAfter))
	}
	if c.DateBefore != nil {
		filters = append(filters, NewDateBeforeFilter(*c.DateBefore))
	}
	return func(l *goaccess.Line) bool {
		for _, f := range filters {
			if !f(l) {
				return false
			}
		}
		return true
	}, nil
}

type Filter func(*goaccess.Line) bool

func AddIfNotEmpty(filters []Filter, v []string, fn func([]string) Filter) []Filter {
	if len(v) == 0 {
		return filters
	}
	return append(filters, fn(v))
}

func NewDateAfterFilter(d time.Time) Filter {
	return func(l *goaccess.Line) bool {
		return l.Timestamp.After(d)
	}
}
func NewDateBeforeFilter(d time.Time) Filter {
	return func(l *goaccess.Line) bool {
		return l.Timestamp.Before(d)
	}
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
