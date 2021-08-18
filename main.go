package main

import (
	"fmt"
	"io"
	"os"

	flag "github.com/spf13/pflag"

	"github.com/floj/logs2goaccess/fetcher"
	"github.com/floj/logs2goaccess/filter"
	"github.com/floj/logs2goaccess/goaccess"
	"github.com/floj/logs2goaccess/transformer"
)

func main() {

	// text := `https 2021-07-26T20:35:37.398983Z app/dev-restricted-alb/550823ca3ef6866f 3.122.136.123:43277 172.19.88.7:443 0.007 0.004 0.000 200 200 189 476 "GET https://vertrag-verlaengern.dev.lab.blau.de:443/manage/healthcheck HTTP/1.1" "Go-http-client/1.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:eu-central-1:121696051528:targetgroup/vertrag-verlaengern-bla-dev-pub/fc26378c97970ba5 "Root=1-60ff1c99-6dbcb5470e32a7e148ea7298" "vertrag-verlaengern.dev.lab.blau.de" "arn:aws:acm:eu-central-1:121696051528:certificate/5cb1023c-4f5d-4e8d-9c64-025eb66ba548" 40173 2021-07-26T20:35:37.387000Z "forward" "-" "-" "172.19.88.7:443" "200" "-" "-"`
	// scn := bufio.NewScanner(strings.NewReader(text))
	// scn.Split(alb.SplitWordsWithQuotes)
	// for scn.Scan() {
	// 	fmt.Printf("#%s#\n", scn.Text())
	// }
	// os.Exit(1)

	printLogFormat := flag.Bool("print-log-format", false, "Print the log-format to use in goaccess")
	printDateFormat := flag.Bool("print-date-format", false, "Print the date-format to use in goaccess")
	printTimeFormat := flag.Bool("print-time-format", false, "Print the time-format to use in goaccess")

	inFmt := flag.String("in-format", "", "format of the data read, possible values are: caddy, aws-elb, aws-cloudfront")

	filterIncludeVHosts := flag.StringSlice("filter-include-vhost", []string{}, "only include logs matching the vhost prefix")
	filterExcludeClientIPs := flag.StringSlice("filter-exclude-client-ip", []string{}, "exclude logs matching the client ip prefix")
	filterExcludeURLs := flag.StringSlice("filter-exclude-url", []string{}, "exclude logs matching the URL prefix")

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

		gl, skip, err := tfmr.Parse(line)
		if err != nil {
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
