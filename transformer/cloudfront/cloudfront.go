package cloudfront

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/floj/logs2goaccess/goaccess"
)

type Parser struct {
}

func (p *Parser) Parse(text string) (*goaccess.Line, bool, error) {
	if strings.HasPrefix(text, "#") {
		return nil, true, nil
	}

	fields := strings.Split(text, "\t")

	// 0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19	20	21	22	23	24	25	26	27	28	19	30	31	32
	// date	time	x-edge-location	sc-bytes	c-ip	cs-method	cs(Host)	cs-uri-stem	sc-status	cs(Referer)	cs(User-Agent)	cs-uri-query	cs(Cookie)	x-edge-result-type	x-edge-request-id	x-host-header	cs-protocol	cs-bytes	time-taken	x-forwarded-for	ssl-protocol	ssl-cipher	x-edge-response-result-type	cs-protocol-version	fle-status	fle-encrypted-fields	c-port	time-to-first-byte	x-edge-detailed-result-type	sc-content-type	sc-content-len	sc-range-start	sc-range-end

	ts, err := time.Parse("2006-01-02 15:04:05", strings.Join(fields[0:2], " "))
	if err != nil {
		return nil, false, err
	}
	respSize, err := strconv.Atoi(fields[3])
	if err != nil {
		return nil, false, err
	}
	respStatus, err := strconv.Atoi(fields[8])
	if err != nil {
		return nil, false, err
	}

	respTime, err := strconv.ParseFloat(fields[18], 64)
	if err != nil {
		return nil, false, err
	}

	path := fields[7]
	if fields[11] != "-" && fields[11] != "" {
		path = path + "?" + fields[11]
	}

	contentType, err := url.PathUnescape(fields[19])
	if err != nil {
		return nil, true, err
	}

	userAgent, err := url.PathUnescape(fields[10])
	if err != nil {
		return nil, true, err
	}

	return &goaccess.Line{
		Timestamp:       ts,
		VHost:           fields[15],
		ClientIP:        fields[4],
		Method:          fields[5],
		URL:             path,
		ResponseStatus:  int(respStatus),
		ResponseSize:    int64(respSize),
		Referer:         fields[9],
		UserAgent:       strings.ReplaceAll(userAgent, "\n", "\\n"),
		ContentType:     strings.ReplaceAll(contentType, "\n", "\\n"),
		RequestDuration: time.Duration(respTime * float64(time.Second)),
	}, false, nil
}
