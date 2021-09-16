package main

import (
	"fmt"
	"io"
	"os"
	"time"

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
	//filterDateBefore := flag.TStringSlice("filter-date-before", []string{}, "exclude logs matching the URL prefix")
	filterDateAfter := flag.String("filter-date-from", "", "only include logs after at this date")
	filterDateBefore := flag.String("filter-date-to", "", "only include logs before this date")
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

	filterConf := filter.FilterConf{
		IncludeHostPrefix:   *filterIncludeVHosts,
		ExcludeClientPrefix: *filterExcludeClientIPs,
		ExcludeURLPrefix:    *filterExcludeURLs,
	}

	flagErrs := []string{}
	if *inFmt == "" {
		flagErrs = append(flagErrs, "--in-format is required")
	}

	{
		d, err := tryParseDate(*filterDateAfter)
		if err != nil {
			flagErrs = append(flagErrs, fmt.Sprintf("--filter-date-from: %v", err))
		}
		filterConf.DateAfter = d
	}
	{
		d, err := tryParseDate(*filterDateBefore)
		if err != nil {
			flagErrs = append(flagErrs, fmt.Sprintf("--filter-date-to: %v", err))
		}
		filterConf.DateBefore = d
	}

	if len(flagErrs) > 0 {
		for _, e := range flagErrs {
			fmt.Println("flag", e)
		}
		os.Exit(1)
	}

	normalizers, err := normalizer.AddIfNotEmpty([]normalizer.Normalizer{}, *normalizeURLs, normalizer.NewURLNormalizer)
	if err != nil {
		panic(err)
	}

	err = run(*inFmt, flag.Args(), filterConf, normalizers, os.Stdout)
	if err != nil {
		panic(err)
	}
}

type stats struct {
	read     int
	skipped  int
	included int
}

func run(inFmt string, locations []string, filterConf filter.FilterConf, normalizers []normalizer.Normalizer, out io.Writer) error {
	filter, err := filterConf.Build()
	if err != nil {
		return err
	}

	in, err := fetcher.ForLocations(locations, filterConf)
	if err != nil {
		return err
	}

	tfmr, err := transformer.ForName(inFmt)
	if err != nil {
		return err
	}

	statsT := time.NewTicker(time.Second * 5)

	statsC := make(chan stats)
	quitC := make(chan struct{})
	defer func() {
		statsT.Stop()
		quitC <- struct{}{}
	}()

	go func() {
		stat := stats{}
		lastStat := stat
		lastTime := time.Now()
		for {
			select {
			case s := <-statsC:
				stat = s
			case t := <-statsT.C:
				delta := stat.read - lastStat.read
				avg := float64(delta) / t.Sub(lastTime).Seconds()
				fmt.Fprintf(os.Stderr, "%d lines read (~%.0f/sec), %d included, %d skipped\n", stat.read, avg, stat.included, stat.skipped)
				lastStat = stat
				lastTime = t
			case <-quitC:
				return
			}
		}
	}()

	stat := stats{}
	start := time.Now()
	// read all lines
	for {
		statsC <- stat
		line, ok, err := in.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		stat.read++

		gl, skip, err := tfmr.Parse(line)
		if err != nil {
			fmt.Fprintln(os.Stderr, "TRANSFORM", err, line)
			stat.skipped++
			continue
		}
		if skip {
			stat.skipped++
			continue
		}

		if !filter(gl) {
			stat.skipped++
			continue
		}
		stat.included++

		for _, normalize := range normalizers {
			gl, err = normalize(gl)
			if err != nil {
				fmt.Fprintln(out, "NORMALIZE", err, line)
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
	fmt.Fprintf(os.Stderr, "%d lines read in %s\n", stat.read, time.Since(start))
	return nil
}

func tryParseDate(v string) (*time.Time, error) {
	if v == "" {
		return nil, nil
	}
	formats := []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006-01-02",
	}
	for _, f := range formats {
		t, err := time.Parse(f, v)
		if err == nil {
			return &t, nil
		}
	}
	return nil, fmt.Errorf("unknown date format: accepted formarts are %v", formats)
}
