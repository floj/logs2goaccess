package main

import (
	"fmt"
	"io"
	"os"

	flag "github.com/spf13/pflag"

	"github.com/floj/logs2goaccess/fetcher"
	"github.com/floj/logs2goaccess/filter"
	"github.com/floj/logs2goaccess/goaccess"
	"github.com/floj/logs2goaccess/normalizer"
	"github.com/floj/logs2goaccess/transformer"
)

func main() {
	printLogFormat := flag.Bool("print-log-format", false, "Print the log-format to use in goaccess")
	printDateFormat := flag.Bool("print-date-format", false, "Print the date-format to use in goaccess")
	printTimeFormat := flag.Bool("print-time-format", false, "Print the time-format to use in goaccess")

	inFmt := flag.String("in-format", "", "format of the data read, possible values are: caddy, aws-elb, aws-cloudfront")

	filterIncludeVHosts := flag.StringSlice("filter-include-vhost", []string{}, "only include logs matching the vhost prefix")
	filterExcludeClientIPs := flag.StringSlice("filter-exclude-client-ip", []string{}, "exclude logs matching the client ip prefix")
	filterExcludeURLs := flag.StringSlice("filter-exclude-url", []string{}, "exclude logs matching the URL prefix")
	normalizeURLs := flag.StringSlice("normalize-url", []string{}, "perform some normalisation on the url")

	flag.Parse()

	if *printLogFormat {
		fmt.Println(goaccess.LineFormat())
		return
	}
	if *printDateFormat {
		fmt.Println(goaccess.DateFormat)
		return
	}
	if *printTimeFormat {
		fmt.Println(goaccess.TimeFormat)
		return
	}

	flagErrs := []string{}
	if *inFmt == "" {
		flagErrs = append(flagErrs, "--in-format is required")
	}
	if len(flagErrs) > 0 {
		for _, e := range flagErrs {
			fmt.Println("flag", e)
		}
		os.Exit(1)
	}

	var err error

	filters := []filter.Filter{}
	filters = filter.AddIfNotEmpty(filters, *filterIncludeVHosts, filter.NewIncludeHostsPrefixFilter)
	filters = filter.AddIfNotEmpty(filters, *filterExcludeURLs, filter.NewExcludeURLsPrefixFilter)
	filters = filter.AddIfNotEmpty(filters, *filterExcludeClientIPs, filter.NewExcludeClientsPrefixFilter)

	normalizers := []normalizer.Normalizer{}
	if normalizers, err = normalizer.AddIfNotEmpty(normalizers, *normalizeURLs, normalizer.NewURLNormalizer); err != nil {
		panic(err)
	}

	locations := flag.Args()
	err = run(*inFmt, locations, filters, normalizers, os.Stdout)
	if err != nil {
		panic(err)
	}
}

func run(inFmt string, locations []string, filters []filter.Filter, normalizers []normalizer.Normalizer, out io.Writer) error {
	in, err := fetcher.ForLocations(locations)
	if err != nil {
		return err
	}

	tfmr, err := transformer.ForName(inFmt)
	if err != nil {
		return err
	}

	// read all lines
	for {
		line, ok, err := in.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		gl, skip, err := tfmr.Parse(line)
		if err != nil {
			fmt.Fprintln(os.Stderr, line)
			return err
		}
		if skip {
			continue
		}

		include := true
		for _, filter := range filters {
			if filter(gl) {
				continue
			}
			include = false
			break
		}
		if !include {
			continue
		}

		for _, normalize := range normalizers {
			gl, err = normalize(gl)
			if err != nil {
				fmt.Fprintln(out, line)
				return err
			}
		}
		_, err = out.Write([]byte(gl.ToGoAccess()))
		if err != nil {
			return err
		}
		_, err = out.Write([]byte("\n"))
		if err != nil {
			return err
		}

	}
	return nil
}
