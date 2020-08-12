package client

//
// http.go
// Author: Abraham Leal
//

import "io"
import "net/http"


func GetNewRequest(method string, endpoint string, key string, secret string, reader io.Reader) *http.Request {
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		panic(err)
	}

	req.SetBasicAuth(key, secret)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "ccloud-schema-exporter/"+Version)
	req.Header.Add("Correlation-Context", "service.name=ccloud-schema-exporter,service.version="+Version)

	return req
}
