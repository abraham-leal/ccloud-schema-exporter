package client

//
// meta.go
// Copyright 2020 Abraham Leal
//

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"net/http"
)

var HttpCallTimeout int
var ScrapeInterval int
var Version = "1.1"
var httpClient http.Client

var SrcSRUrl string
var SrcSRKey string
var SrcSRSecret string
var DestSRUrl string
var DestSRKey string
var DestSRSecret string
var CustomDestinationName string
var CustomSourceName string
var NoPrompt bool
var SyncDeletes bool
var SyncHardDeletes bool
var ThisRun RunMode
var PathToWrite string
var CancelRun bool
var WithMetrics bool
var AllowList StringArrayFlag
var DisallowList StringArrayFlag

// Define RunMode Enum
type RunMode int

const (
	SYNC RunMode = iota
	BATCH
	TOLOCAL
	FROMLOCAL
)

func (r RunMode) String() string {
	return [...]string{"SYNC", "BATCH", "TOLOCAL", "FROMLOCAL"}[r]
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

// Custom Metrics
var (
	schemasRegistered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "schema_exporter_registered_schemas",
		Help: "The total number of registered schemas",
	})
)

var (
	schemasSoftDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "schema_exporter_softDeleted_schemas",
		Help: "The total number of soft deleted schemas",
	})
)

var (
	schemasHardDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "schema_exporter_hardDeleted_schemas",
		Help: "The total number of hard deleted schemas",
	})
)
