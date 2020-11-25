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
var Version = "1.1-SNAPSHOT"
var httpClient http.Client

var SrcSRUrl string
var SrcSRKey string
var SrcSRSecret string
var DestSRUrl string
var DestSRKey string
var DestSRSecret string
var CustomDestinationName string
var NoPrompt bool
var SyncDeletes bool
var SyncHardDeletes bool
var ThisRun RunMode
var PathToWrite string
var CancelRun bool
var AllowList StringArrayFlag
var DisallowList StringArrayFlag

// Define RunMode Enum
type RunMode int

const (
	SYNC RunMode = iota
	BATCH
	LOCAL
)

func (r RunMode) String() string {
	return [...]string{"SYNC", "BATCH", "LOCAL"}[r]
}

// Define Mode Enum
type Mode int

const (
	IMPORT Mode = iota
	READONLY
	READWRITE
)

func (m Mode) String() string {
	return [...]string{"IMPORT", "READONLY", "READWRITE"}[m]
}

// Define Compatibility Enum
type Compatibility int

const (
	BACKWARD Compatibility = iota
	BACKWARD_TRANSITIVE
	FORWARD
	FORWARD_TRANSITIVE
	FULL
	FULL_TRANSITIVE
	NONE
)

func (c Compatibility) String() string {
	return [...]string{"BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE"}[c]
}
