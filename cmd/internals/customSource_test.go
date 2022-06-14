package client

//
// customSource_test.go
// Copyright 2020 Abraham Leal
//

import (
	"github.com/stretchr/testify/assert"
	"log"
	"reflect"
	"testing"
	"time"
)

var schemaToReferenceDTwo = "{\"type\":\"record\",\"name\":\"referenceNextLevel\",\"namespace\":\"com.reference\",\"fields\":[{\"name\":\"someField\",\"type\":\"string\"}]}"

var schemaToReferenceDOne = "{\"type\":\"record\",\"name\":\"reference\",\"namespace\":\"com.reference\",\"fields\":[{\"name\":\"someField\",\"type\":\"com.reference.referenceNextLevel\"}]}"

var schemaReferencing = "{\"type\":\"record\",\"name\":\"sampleRecordreferencing\",\"namespace\":\"com.mycorp.somethinghere\",\"fields\":[{\"name\":\"reference\",\"type\":\"com.reference.reference\"}]}"

var referenceDepthTwo = SchemaRecord{
	Subject:    "referenceNextLevel",
	Schema:     schemaToReferenceDTwo,
	SType:      "AVRO",
	Version:    1,
	Id:         998,
	References: nil,
}

var DTwoReference = SchemaReference{
	Name:    "com.reference.referenceNextLevel",
	Subject: "referenceNextLevel",
	Version: 1,
}

var referenceDepthOne = SchemaRecord{
	Subject:    "reference",
	Schema:     schemaToReferenceDOne,
	SType:      "AVRO",
	Version:    1,
	Id:         999,
	References: []SchemaReference{DTwoReference},
}
var DOneReference = SchemaReference{
	Name:    "com.reference.reference",
	Subject: "reference",
	Version: 1,
}

var schema1 = SchemaRecord{
	Subject:    testingSubject,
	Schema:     schemaReferencing,
	SType:      "AVRO",
	Version:    1,
	Id:         10001,
	References: []SchemaReference{DOneReference},
}

func TestMainStackCustomSource(t *testing.T) {
	setup()
	t.Run("TCustomSourceBatch", func(t *testing.T) { TCustomSourceBatch(t) })
	t.Run("TCustomSourceSync", func(t *testing.T) { TCustomSourceSync(t) })
	tearDown()
}

func TCustomSourceBatch(t *testing.T) {
	testClient.DeleteAllSubjectsPermanently()

	CancelRun = false
	log.Println("Testing Custom Source in Batch Mode")

	myTestCustomSource := NewInMemRegistry([]SchemaRecord{schema1, referenceDepthOne, referenceDepthTwo})

	RunCustomSourceBatch(testClient, &myTestCustomSource)

	subjectState := GetCurrentSubjectState(testClient)

	assert.True(t, reflect.DeepEqual(subjectState[myTestCustomSource.inMemSchemas[referenceDepthTwo.Id].Subject], []int64{1}))
	assert.True(t, reflect.DeepEqual(subjectState[myTestCustomSource.inMemSchemas[referenceDepthOne.Id].Subject], []int64{1}))
	assert.True(t, reflect.DeepEqual(subjectState[myTestCustomSource.inMemSchemas[schema1.Id].Subject], []int64{1}))

	testClient.DeleteAllSubjectsPermanently()
}

func TCustomSourceSync(t *testing.T) {
	testClient.DeleteAllSubjectsPermanently()

	log.Println("Testing Custom Source in Sync Mode")

	myTestCustomSource := NewInMemRegistry([]SchemaRecord{schema1, referenceDepthOne, referenceDepthTwo})

	ScrapeInterval = 3
	CancelRun = false
	SyncDeletes = true
	go RunCustomSourceSync(testClient, &myTestCustomSource)
	time.Sleep(time.Duration(5) * time.Second) // Give time for sync

	subjectState := GetCurrentSubjectState(testClient)

	assert.True(t, reflect.DeepEqual(subjectState[myTestCustomSource.inMemSchemas[referenceDepthTwo.Id].Subject], []int64{1}))
	assert.True(t, reflect.DeepEqual(subjectState[myTestCustomSource.inMemSchemas[referenceDepthOne.Id].Subject], []int64{1}))
	assert.True(t, reflect.DeepEqual(subjectState[myTestCustomSource.inMemSchemas[schema1.Id].Subject], []int64{1}))

	// Test deletes sync, this should delete the schema specified and any that reference it.
	delete(myTestCustomSource.inMemSchemas, referenceDepthOne.Id)
	delete(myTestCustomSource.inMemSchemas, schema1.Id)
	time.Sleep(time.Duration(5) * time.Second) // Give time for sync

	assert.Equal(t, 1, len(myTestCustomSource.inMemSchemas))
	assert.Equal(t, len(myTestCustomSource.inMemSchemas), len(testClient.getSchemaList(false)))

	CancelRun = true
	time.Sleep(time.Duration(3) * time.Second) // Give time for killing goroutine

	testClient.DeleteAllSubjectsPermanently()
}
