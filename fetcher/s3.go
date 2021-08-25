package fetcher

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/floj/logs2goaccess/filter"
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

func s3LocationResolver(loc string) ([]string, error) {
	loc = strings.TrimPrefix(loc, "s3:")
	if !strings.HasPrefix(loc, "recurse:") {
		// remove '//' prefix if present as in s3://my-bucket
		loc = strings.TrimPrefix(loc, "//")
		return []string{loc}, nil
	}

	client, err := getS3Client()
	if err != nil {
		return nil, err
	}
	loc = strings.TrimPrefix(loc, "recurse:")
	// remove '//' prefix if present as in s3:recurse://my-bucket
	loc = strings.TrimPrefix(loc, "//")

	locs := []string{}

	loc, matcher, err := findMatchers(loc)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(loc, "/")
	bucket := parts[0]

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
			// skip empty files
			if c.Size == 0 {
				continue
			}
			if !matcher(*c.Key) {
				continue
			}
			locs = append(locs, fmt.Sprintf("s3:%s/%s", bucket, *c.Key))
		}
	}
	return locs, nil
}

func s3Reader(loc string, fc filter.FilterConf) (io.ReadCloser, error) {
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
}
