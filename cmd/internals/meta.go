package client

//
// meta.go
// Author: Abraham Leal
//

import (
	"net/http"
)

var httpCallTimeout int
var ScrapeInterval int
var Version = "0.3-SNAPSHOT"
var httpClient http.Client

var SrcSRUrl string
var SrcSRKey string
var SrcSRSecret string
var DestSRUrl string
var DestSRKey string
var DestSRSecret string
var RunMode string
var syncDeletes bool

