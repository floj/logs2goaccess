package fetcher

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/floj/logs2goaccess/filter"
)

var cwLogsClientOnce sync.Once
var cwLogsClient *cloudwatchlogs.Client

func getCwLogsClient() (*cloudwatchlogs.Client, error) {
	var err error
	cwLogsClientOnce.Do(func() {
		var cfg aws.Config
		cfg, err = config.LoadDefaultConfig(context.Background())
		if err != nil {
			return
		}
		cwLogsClient = cloudwatchlogs.NewFromConfig(cfg)
	})
	return cwLogsClient, err
}

func cwLogsResolver(loc string) ([]string, error) {
	return []string{loc}, nil
}

type cwlReader struct {
	pager *cloudwatchlogs.FilterLogEventsPaginator
	buf   *bytes.Buffer
}

func (r *cwlReader) Close() error {
	return nil
}

func (r *cwlReader) Read(p []byte) (int, error) {
	if r.buf == nil {
		if !r.pager.HasMorePages() {
			return 0, io.EOF
		}
		page, err := r.pager.NextPage(context.Background())
		if err != nil {
			return 0, err
		}
		r.buf = &bytes.Buffer{}
		for _, e := range page.Events {
			r.buf.WriteString(*e.Message)
			r.buf.WriteByte('\n')
		}
	}

	n, err := r.buf.Read(p)
	if err == io.EOF {
		r.buf = nil
		err = nil
	}
	return n, err
}

func cwLogsReader(loc string, fc filter.FilterConf) (io.ReadCloser, error) {
	group := strings.TrimPrefix(loc, "cwlogs:")
	c, err := getCwLogsClient()
	if err != nil {
		return nil, err
	}

	req := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName: &group,
	}
	if fc.DateAfter != nil {
		req.StartTime = aws.Int64(fc.DateAfter.UnixMilli())
	}
	if fc.DateBefore != nil {
		req.EndTime = aws.Int64(fc.DateBefore.UnixMilli())
	}
	pager := cloudwatchlogs.NewFilterLogEventsPaginator(c, req)

	return &cwlReader{pager: pager}, nil

}
