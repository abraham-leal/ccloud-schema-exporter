package client

//
// localFSFunctions_test.go
// Copyright 2020 Abraham Leal
//

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFileName(t *testing.T) {
	regularFilename := "/tmp/xmlToAvro-key-1-10382-AVRO"
	trickyFileName := "/tmp/myNew-Topic-value-1-35421-JSON"
	currentDirFileName := "someTopic-value-1-65421-PROTO"
	windowsFileName := "D:\\Schematest\\SchemaRegistryBackup\\myNew-Topic-value-1-35421-JSON"

	regularID, regularVersion, regularSubject, regularSType := parseFileName(regularFilename)
	trickyID, trickyVersion, trickySubject, trickySType := parseFileName(trickyFileName)
	currentDirID, currentDirVersion, currentDirSubject, currentDirSType := parseFileName(currentDirFileName)
	windowsID, windowsVersion, windowsSubject, windowsSType := parseFileName(windowsFileName)

	assert.True(t, regularID == int64(10382))
	assert.True(t, regularVersion == int64(1))
	assert.True(t, regularSubject == "xmlToAvro-key")
	assert.True(t, regularSType == "AVRO")
	assert.True(t, trickyID == int64(35421))
	assert.True(t, trickyVersion == int64(1))
	assert.True(t, trickySubject == "myNew-Topic-value")
	assert.True(t, trickySType == "JSON")
	assert.True(t, currentDirID == int64(65421))
	assert.True(t, currentDirVersion == int64(1))
	assert.True(t, currentDirSubject == "someTopic-value")
	assert.True(t, currentDirSType == "PROTO")
	assert.True(t, windowsID == int64(35421))
	assert.True(t, windowsVersion == int64(1))
	assert.True(t, windowsSubject == "myNew-Topic-value")
	assert.True(t, windowsSType == "JSON")

}
