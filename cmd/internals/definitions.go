package client

//
// definitions.go
// Author: Abraham Leal
//

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"unicode"
)

type SchemaRegistryClient struct {
	SRUrl              string
	SRApiKey           string
	SRApiSecret        string
	InMemSchemas       map[string][]int64
	srcInMemDeletedIDs map[int64]map[string]int64
}

/*
Any struct that implements this interface is able to run an instance of sync and batch exporting.
*/
type CustomDestination interface {
	// Perform any set-up behavior before start of sync/batch export
	SetUp() error
	// An implementation should handle the registration of a schema in the destination.
	// The SchemaRecord struct provides all details needed for registration.
	RegisterSchema(record SchemaRecord) error
	// An implementation should handle the deletion of a schema in the destination.
	// The SchemaRecord struct provides all details needed for deletion.
	DeleteSchema(record SchemaRecord) error
	// An implementation should be able to send exactly one map describing the state of the destination
	// This map should be minimal. Describing only the Subject and Versions that already exist.
	GetDestinationState() (map[string][]int64, error)
	// Perform any tear-down behavior before stop of sync/batch export
	TearDown() error
}

/*
Any struct that implements this interface is able to run an instance of sync and batch exporting.
*/
type CustomSource interface {
	// Perform any set-up behavior before start of sync/batch export
	SetUp() error
	// An implementation should handle the retrieval of a schema from the source.
	// The id should be a unique identifier for the schema.
	GetSchema(SchemaSourceID int64) (subject string, version int64, id int64, stype string, schema string, err error)
	// An implementation should be able to send exactly one map describing the state of the source
	// This map should be minimal. Describing only the Subject and Versions that exist.
	GetSourceState() (map[string][]int64, error)
	// Perform any tear-down behavior before stop of sync/batch export
	TearDown() error
}

type SchemaRecord struct {
	Subject string `json:"subject"`
	Schema  string `json:"schema"`
	SType   string `json:"schemaType"`
	Version int64  `json:"version"`
	Id      int64  `json:"id"`
}

//Constructor to assure Type-less schemas get registered with Avro
func (srs SchemaRecord) setTypeIfEmpty() SchemaRecord {
	if srs.SType == "" {
		srs.SType = "AVRO"
	}

	return srs
}

type SchemaToRegister struct {
	Schema  string `json:"schema"`
	Id      int64  `json:"id,omitempty"`
	Version int64  `json:"version,omitempty"`
	SType   string `json:"schemaType"`
}

type SchemaExtraction struct {
	Schema  string `json:"schema"`
	Id      int64  `json:"id"`
	Version int64  `json:"version"`
	Subject string `json:"subject"`
}

type ModeRecord struct {
	Mode string `json:"mode"`
}

type CompatRecord struct {
	Compatibility string `json:"compatibility"`
}

type SubjectWithVersions struct {
	Subject  string
	Versions []int64
}

type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int64  `json:"version"`
}

type StringArrayFlag map[string]bool

func (i *StringArrayFlag) String() string {
	return fmt.Sprintln(*i)
}

func (i *StringArrayFlag) Set(value string) error {
	currentPath, _ := os.Getwd()

	if strings.LastIndexAny(value, "/.") != -1 {
		path := CheckPath(value, currentPath)
		f, err := ioutil.ReadFile(path)
		if err != nil {
			panic(err)
		}
		value = string(f)
	}

	nospaces := i.removeSpaces(value)

	tempMap := map[string]bool{}

	for _, s := range strings.Split(nospaces, ",") {
		tempMap[s] = true
	}

	*i = tempMap

	return nil
}

func (i *StringArrayFlag) removeSpaces(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}

type idSubjectVersion struct {
	Id                int64            `json:"Id"`
	SubjectAndVersion map[string]int64 `json:"SubjectAndVersion"`
}
