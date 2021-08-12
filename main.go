package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/floj/logs2goaccess/fetcher"
	"github.com/floj/logs2goaccess/filter"
	"github.com/floj/logs2goaccess/goaccess"
	"github.com/floj/logs2goaccess/transformer"
)

func main() {
	printLogFormat := flag.Bool("print-log-format", false, "Print the log-format to use in goaccess")

	inFmt := flag.String("in-format", "", "format of the data read, possible values are: caddy, aws-elb, aws-cloudfront")

	filterIncludeVHosts := flag.String("filter-include-vhosts", "", "only include logs matching the vhost prefix (comma separated)")
	filterExcludeClientIPs := flag.String("filter-exclude-client-ips", "", "exclude logs matching the client ip prefixes (comma separated)")
	filterExcludeURLs := flag.String("filter-exclude-urls", "", "exclude logs matching the URL prefixes (comma separated)")

	flag.Parse()

	if *printLogFormat {
		fmt.Println(goaccess.LineFormat)
		return
	}

	flagErrs := []string{}
	if *inFmt == "" {
		flagErrs = append(flagErrs, "-in-format is required")
	}
	if len(flagErrs) > 0 {
		for _, e := range flagErrs {
			fmt.Println("flag", e)
		}
		os.Exit(1)
	}

	filters := []filter.Filter{}
	filters = filter.AddFilterIfNotEmpty(filters, *filterIncludeVHosts, filter.NewIncludeHostsPrefixFilter)
	filters = filter.AddFilterIfNotEmpty(filters, *filterExcludeURLs, filter.NewExcludeURLsPrefixFilter)
	filters = filter.AddFilterIfNotEmpty(filters, *filterExcludeClientIPs, filter.NewExcludeClientsPrefixFilter)

	locations := flag.Args()
	err := run(*inFmt, locations, filters, os.Stdout)
	if err != nil {
		panic(err)
	}
}

func run(inFmt string, locations []string, filters []filter.Filter, out io.Writer) error {
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

		gl, err := tfmr.Parse(line)
		if err != nil {
			return err
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
