package transformer

import (
	"fmt"

	"github.com/floj/logs2goaccess/goaccess"
	"github.com/floj/logs2goaccess/transformer/alb"
	"github.com/floj/logs2goaccess/transformer/caddy"
	"github.com/floj/logs2goaccess/transformer/cloudfront"
)

type Transformer interface {
	Parse(line string) (*goaccess.Line, bool, error)
}

var factories = map[string]func() (Transformer, error){
	"caddy":          func() (Transformer, error) { return &caddy.Parser{}, nil },
	"aws:cloudfront": func() (Transformer, error) { return &cloudfront.Parser{}, nil },
	"aws:alb":        func() (Transformer, error) { return &alb.Parser{}, nil },
}

func ForName(name string) (Transformer, error) {
	fn, set := factories[name]
	if !set {
		names := []string{}
		for n := range factories {
			names = append(names, n)
		}
		return nil, fmt.Errorf("no transformer for '%s' found. Known transformers: %v", name, names)
	}
	return fn()
}
