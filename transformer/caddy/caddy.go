package caddy

import (
	"encoding/json"
	"mime"
	"net"
	"strings"
	"time"

	"github.com/floj/logs2goaccess/goaccess"
	"github.com/floj/logs2goaccess/transformer/utils"
)

type Parser struct {
}

func (p *Parser) Parse(text string) (*goaccess.Line, bool, error) {
	cl := caddyLog{}
	err := json.Unmarshal([]byte(text), &cl)
	if err != nil {
		return nil, false, err
	}

	// normalize headers
	reqHeaders := utils.HeadersFromMap(cl.Request.Headers)
	respHeaders := utils.HeadersFromMap(cl.RespHeaders)

	clientIP, _, _ := net.SplitHostPort(cl.Request.RemoteAddr)
	if xff := reqHeaders.Get("x-forwarded-for"); xff != "" {
		parts := strings.Split(xff, ",")
		clientIP = strings.TrimSpace(parts[len(parts)-1])
	}

	contentType := ""
	ctHeader := respHeaders.Get("content-type")
	if ctHeader != "" {
		contentType, _, _ = mime.ParseMediaType(respHeaders.Get("content-type"))
	}
	return &goaccess.Line{
		Timestamp:       time.Unix(int64(cl.Ts), 0),
		VHost:           cl.Request.Host,
		ClientIP:        clientIP,
		Method:          cl.Request.Method,
		URL:             cl.Request.URI,
		ResponseStatus:  cl.Status,
		ResponseSize:    int64(cl.Size),
		Referer:         reqHeaders.Get("referer"),
		UserAgent:       reqHeaders.Get("user-agent"),
		ContentType:     contentType,
		RequestDuration: time.Duration(cl.Duration * float64(time.Second)),
	}, false, nil
}

type caddyLog struct {
	Ts      float64 `json:"ts"`
	Logger  string  `json:"logger"`
	Msg     string  `json:"msg"`
	Request struct {
		RemoteAddr string              `json:"remote_addr"`
		Proto      string              `json:"proto"`
		Method     string              `json:"method"`
		Host       string              `json:"host"`
		URI        string              `json:"uri"`
		Headers    map[string][]string `json:"headers"`

		TLS struct {
			Resumed     bool   `json:"resumed"`
			Version     int    `json:"version"`
			CipherSuite int    `json:"cipher_suite"`
			Proto       string `json:"proto"`
			ProtoMutual bool   `json:"proto_mutual"`
			ServerName  string `json:"server_name"`
		} `json:"tls"`
	} `json:"request"`
	Duration    float64             `json:"duration"`
	Size        int                 `json:"size"`
	Status      int                 `json:"status"`
	RespHeaders map[string][]string `json:"resp_headers"`
}
