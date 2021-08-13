package goaccess

import (
	"strconv"
	"strings"
	"time"
)

/*
%x A date and time field matching the time-format and date-format variables. This is used when a timestamp is given instead of the date and time being in two separate variables.
%t time field matching the time-format variable.
%d date field matching the date-format variable.
%v The server name according to the canonical name setting (Server Blocks or Virtual Host).
%e This is the userid of the person requesting the document as determined by HTTP authentication.
%C The cache status of the object the server served.
%h host (the client IP address, either IPv4 or IPv6)
%r The request line from the client. This requires specific delimiters around the request (single quotes, double quotes, etc) to be parsable. Otherwise, use a combination of special format specifiers such as %m, %U, %q and %H to parse individual fields. Note: Use either %r to get the full request OR %m, %U, %q and %H to form your request, do not use both.
%m The request method.
%U The URL path requested. Note: If the query string is in %U, there is no need to use %q. However, if the URL path, does not include any query string, you may use %q and the query string will be appended to the request.
%q The query string.
%H The request protocol.
%s The status code that the server sends back to the client.
%b The size of the object returned to the client.
%R The "Referer" HTTP request header.
%u The user-agent HTTP request header.
%K The TLS encryption settings chosen for the connection. (In Apache LogFormat: %{SSL_PROTOCOL}x).
%k The TLS encryption settings chosen for the connection. (In Apache LogFormat: %{SSL_CIPHER}x).
%M The MIME-type of the requested resource. (In Apache LogFormat: %{Content-Type}o)
%D The time taken to serve the request, in microseconds.
%T The time taken to serve the request, in seconds with milliseconds resolution.
%L The time taken to serve the request, in milliseconds as a decimal number.
%^ Ignore this field.
%~ Move forward through the log string until a non-space (!isspace) char is found.
~h The host (the client IP address, either IPv4 or IPv6) in a X-Forwarded-For (XFF) field.
*/

type Line struct {
	Timestamp       time.Time     // %d %t
	VHost           string        // %v
	Username        string        // %e
	ClientIP        string        // %h
	Method          string        // %m
	URL             string        // %U
	ResponseStatus  int           // %s
	ResponseSize    int64         // %b
	Referer         string        // %R
	UserAgent       string        // %u
	TLSProtocol     string        // %K
	TLSCipher       string        // %k
	ContentType     string        // %M
	RequestDuration time.Duration // %L
}

const (
	DateFormat = `%Y-%m-%d`
	TimeFormat = `%H:%M:%S`

	localDate = "2006-01-02"
	localTime = "15:04:05"
)

func LineFormat() string {
	fields := []string{
		"%d",
		"%t",
		"%v",
		"%e",
		"%h",
		"%m",
		"%U",
		"%s",
		"%b",
		"%R",
		"%u",
		"%K",
		"%k",
		"%M",
		"%L",
	}
	return strings.Join(fields, `\t`)
}

func (l *Line) ToGoAccess() string {
	fields := []string{
		/* %d */ l.Timestamp.Format(localDate),
		/* %t */ l.Timestamp.Format(localTime),
		/* %v */ l.VHost,
		/* %e */ l.Username,
		/* %h */ l.ClientIP,
		/* %m */ l.Method,
		/* %U */ l.URL,
		/* %s */ strconv.Itoa(l.ResponseStatus),
		/* %b */ strconv.FormatInt(l.ResponseSize, 10),
		/* %R */ l.Referer,
		/* %u */ l.UserAgent,
		/* %K */ l.TLSProtocol,
		/* %k */ l.TLSCipher,
		/* %M */ l.ContentType,
		/* %L */ strconv.FormatInt(l.RequestDuration.Milliseconds(), 10),
	}
	return strings.Join(fields, "\t")
}
