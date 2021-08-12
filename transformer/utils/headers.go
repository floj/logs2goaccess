package utils

import "net/http"

func HeadersFromMap(m map[string][]string) http.Header {
	h := http.Header{}
	for k, vv := range m {
		for _, v := range vv {
			h.Add(k, v)
		}
	}
	return h
}
