package alb

import (
	"bufio"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/floj/logs2goaccess/goaccess"
)

type Parser struct {
}

// Modified version of bufio.ScanWords either splits on words or sentences in quotes
func SplitWordsWithQuotes(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Skip leading spaces.
	start := 0
	for start < len(data) {
		c := data[start]
		if c != ' ' {
			break
		}
		start++
	}
	endMarker := ' '
	if len(data) > 0 && data[start] == '"' {
		endMarker = '"'
		start += 1
	}
	for i := start; i < len(data); i++ {
		if data[i] == byte(endMarker) {
			return i + 1, data[start:i], nil
		}
	}

	// If we're at EOF, we have a final, non-empty, non-terminated word. Return it.
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}
	// Request more data.
	return start, nil, nil
}

func (p *Parser) Parse(text string) (*goaccess.Line, bool, error) {

	// 0    1             2   3           4           5                       6                      7                        8               9                  10             11         12        13           14         15           16               17         18            19                20                    21                    22                 23             24             25                 26                        27               28
	// type time(iso8601) elb client:port target:port request_processing_time target_processing_time response_processing_time elb_status_code target_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol target_group_arn "trace_id" "domain_name" "chosen_cert_arn" matched_rule_priority request_creation_time "actions_executed" "redirect_url" "error_reason" "target:port_list" "target_status_code_list" "classification" "classification_reason"
	// request = HTTP method + protocol://host:port/uri + HTTP version
	//      eg   "GET https://interview.dev.lab.o2online.de:443/ HTTP/1.1"

	scn := bufio.NewScanner(strings.NewReader(text))
	scn.Split(SplitWordsWithQuotes)

	fields := []string{}
	for scn.Scan() {
		fields = append(fields, scn.Text())
	}
	if scn.Err() != nil {
		return nil, false, scn.Err()
	}

	ts, err := time.Parse(time.RFC3339Nano, fields[1])
	if err != nil {
		return nil, false, err
	}

	clientIP, _, err := net.SplitHostPort(fields[3])
	if err != nil {
		return nil, false, err
	}

	reqParts := strings.Split(fields[12], " ")
	uri, err := url.Parse(reqParts[1])
	if err != nil {
		//return nil, false, err
		// if the URL can not be parsed, skip the entry rather than erroring
		return nil, true, nil
	}

	respSize, err := strconv.Atoi(fields[11])
	if err != nil {
		return nil, false, err
	}
	respStatus, err := strconv.Atoi(fields[8])
	if err != nil {
		return nil, false, err
	}

	respTime, err := sumTimes(fields[5], fields[6], fields[7])
	if err != nil {
		return nil, false, err
	}

	path := uri.EscapedPath()
	if uri.RawQuery != "" {
		path = path + "?" + uri.RawQuery
	}

	return &goaccess.Line{
		Timestamp:       ts,
		VHost:           fields[18],
		ClientIP:        clientIP,
		Method:          reqParts[0],
		URL:             path,
		ResponseStatus:  int(respStatus),
		ResponseSize:    int64(respSize),
		Referer:         "",
		UserAgent:       fields[13],
		ContentType:     "",
		RequestDuration: respTime,
	}, false, nil
}

func sumTimes(s ...string) (time.Duration, error) {
	secs := 0.
	for _, v := range s {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return time.Duration(0), err
		}
		secs += f
	}
	return time.Duration(secs * float64(time.Second)), nil
}
