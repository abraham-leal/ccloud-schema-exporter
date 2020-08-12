package client

//
// meta.go
// Author: Abraham Leal
//

import "net/http"

var httpCallTimeout int
var scrapeInterval int
var Version string = "0.1"
var httpClient http.Client

var SrcSRUrl string
var SrcSRKey string
var SrcSRSecret string
var DestSRUrl string
var DestSRKey string
var DestSRSecret string

