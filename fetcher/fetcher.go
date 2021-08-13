package fetcher

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var s3ClientOnce sync.Once
var s3Client *s3.Client

func getS3Client() (*s3.Client, error) {
	var err error
	s3ClientOnce.Do(func() {
		var cfg aws.Config
		cfg, err = config.LoadDefaultConfig(context.Background())
		if err != nil {
			return
		}
		s3Client = s3.NewFromConfig(cfg)
	})
	return s3Client, err
}

type Fetcher interface {
	io.Closer
	Next() (line string, lineRead bool, err error)
}

type FetcherImpl struct {
	locations []string
	current   io.ReadCloser
	s         *bufio.Scanner
}

func (f *FetcherImpl) Close() error {
	if f.current == nil {
		return nil
	}
	return f.Close()
}

var factories = map[string]func(string) (io.ReadCloser, error){
	"file:": func(path string) (io.ReadCloser, error) {
		in, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return wrapGzipIfRequired(in, path)
	},
	"s3:": func(loc string) (io.ReadCloser, error) {
		client, err := getS3Client()
		if err != nil {
			return nil, err
		}

		parts := strings.Split(loc, "/")
		resp, err := client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: &parts[0],
			Key:    aws.String(strings.Join(parts[1:], "/")),
		})
		if err != nil {
			return nil, err
		}
		return wrapGzipIfRequired(resp.Body, loc)
	},
}

type locationResolver func(s string) ([]string, error)

var locationResolvers = map[string]func(s string) ([]string, error){
	"file:": func(loc string) ([]string, error) { return []string{loc}, nil },
	"s3:": func(loc string) ([]string, error) {

		loc = strings.TrimPrefix(loc, "s3:")
		if !strings.HasPrefix(loc, "recurse:") {
			return []string{loc}, nil
		}

		client, err := getS3Client()
		if err != nil {
			return nil, err
		}
		loc = strings.TrimPrefix(loc, "recurse:")

		locs := []string{}

		parts := strings.Split(loc, "/")
		bucket := parts[0]

		lastPart := parts[len(parts)-1]
		matcher, stripPart, err := matcherFromPart(lastPart)
		if err != nil {
			return nil, err
		}
		if stripPart {
			parts = parts[0 : len(parts)-1]
		}

		paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: &bucket,
			Prefix: aws.String(strings.Join(parts[1:], "/")),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.Background())
			if err != nil {
				return nil, err
			}
			for _, c := range page.Contents {
				if !matcher(*c.Key) {
					continue
				}
				locs = append(locs, fmt.Sprintf("s3:%s/%s", bucket, *c.Key))
			}
		}
		return locs, nil
	},
}

func resolverFor(loc string) (locationResolver, bool) {
	for prefix, resolver := range locationResolvers {
		if strings.HasPrefix(loc, prefix) {
			return resolver, true
		}
	}
	return nil, false
}

func ForLocations(locations []string) (Fetcher, error) {
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
	return &FetcherImpl{locations: locs}, nil
}

func (f *FetcherImpl) Next() (string, bool, error) {
	if len(f.locations) == 0 {
		return "", false, nil
	}
	if f.s == nil {
		r, err := open(f.locations[0])
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

func open(location string) (io.ReadCloser, error) {
	fmt.Fprintf(os.Stderr, "opening %s\n", location)
	for p, fn := range factories {
		if strings.HasPrefix(location, p) {
			s := strings.TrimPrefix(location, p)
			return fn(s)
		}
	}

	prefixes := []string{}
	for p := range factories {
		prefixes = append(prefixes, p)
	}
	return nil, fmt.Errorf("no fetcher for '%s' found, supported prefixes are %v", location, prefixes)
}

func matcherFromPart(part string) (func(string) bool, bool, error) {
	if strings.HasPrefix(part, "rexexp:") {
		pattern := strings.TrimPrefix(part, "regexp:")
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, false, err
		}
		return re.MatchString, true, nil
	}

	if strings.HasPrefix(part, "suffix:") {
		suf := strings.TrimPrefix(part, "suffix:")
		return func(s string) bool { return strings.HasSuffix(s, suf) }, true, nil
	}

	return func(s string) bool { return true }, false, nil
}
