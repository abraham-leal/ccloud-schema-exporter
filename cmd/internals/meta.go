package client

//
// meta.go
// Author: Abraham Leal
//

import (
	"net/http"
)

var HttpCallTimeout int
var ScrapeInterval int
var Version = "0.4"
var httpClient http.Client

var SrcSRUrl string
var SrcSRKey string
var SrcSRSecret string
var DestSRUrl string
var DestSRKey string
var DestSRSecret string
var RunMode string
var SyncDeletes bool
var SyncHardDeletes bool
var PathToWrite string
var TestHarnessRun bool
var LowerBound int64
var UpperBound int64
var AllowList StringArrayFlag
var DisallowList StringArrayFlag
