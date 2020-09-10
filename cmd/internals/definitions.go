package client

import (
	"fmt"
	"io/ioutil"
	"strings"
	"unicode"
)

//
// definitions.go
// Author: Abraham Leal
//

type SchemaRegistryClient struct {
	SRUrl        string
	SRApiKey     string
	SRApiSecret  string
	InMemSchemas map[string][]int64
	InMemIDs	 map[int64]map[string]int64
}

type SchemaRecord struct {
	Subject string `json:"subject"`
	Schema string  `json:"schema"`
	SType string   `json:"schemaType"`
	Version int64  `json:"version"`
	Id int64	   `json:"id"`
}

//Constructor to assure Type-less schemas get registered with Avro
func (srs SchemaRecord) setTypeIfEmpty () SchemaRecord {
	if (srs.SType == ""){
		srs.SType = "AVRO"
	}

	return srs
}

type SchemaToRegister struct {
	Schema string 	`json:"schema"`
	Id int64 			`json:"id"`
	Version int64 	`json:"version"`
	SType string 	`json:"schemaType"`
}

type SchemaExtraction struct {
	Schema string 	`json:"schema"`
	Id int 			`json:"id"`
	Version int64 	`json:"version"`
	Subject string 	`json:"subject"`
}

type ModeRecord struct {
	Mode string 	`json:"mode"`
}

type SubjectWithVersions struct {
	Subject string
	Versions []int64
}

type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int64 `json:"version"`
}

type StringArrayFlag map[string]bool

func (i *StringArrayFlag) String() string {
	return fmt.Sprintln(*i)
}

func (i *StringArrayFlag) Set(value string) error {

	if strings.LastIndexAny(value,"/.") != -1 {
		path := CheckPath(value)
		f , err := ioutil.ReadFile(path)
		if err != nil {
			panic(err)
		}
		value = string(f)
	}

	nospaces := i.removeSpaces(value)

	tempMap := map[string]bool{}

	for _, s := range strings.Split(nospaces,",") {
		tempMap[s] = true
	}

	*i = tempMap

	return nil
}

func (i *StringArrayFlag) removeSpaces (str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}