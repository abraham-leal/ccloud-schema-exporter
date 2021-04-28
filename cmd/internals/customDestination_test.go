package client

//
// customDestination_test.go
// Copyright 2020 Abraham Leal
//

import (
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestMainStackCustomDestination(t *testing.T) {
	setup()
	t.Run("TCustomDestinationBatch", func(t *testing.T) { TCustomDestinationBatch(t) })
	t.Run("TCustomDestinationSync", func(t *testing.T) { TCustomDestinationSync(t) })
	tearDown()
}

func TCustomDestinationBatch(t *testing.T) {
	log.Println("Testing Custom Destination in Batch Mode")
	testClient.RegisterSchemaBySubjectAndIDAndVersion(mockSchema, testingSubject, 10001, 1, "AVRO", []SchemaReference{})
	myTestCustomDestination := NewSampleCustomDestination()

	RunCustomDestinationBatch(testClient, &myTestCustomDestination)

	_, exists := myTestCustomDestination.inMemState[testingSubject]

	assert.True(t, exists)
}

func TCustomDestinationSync(t *testing.T) {
	log.Println("Testing Custom Destination in Sync Mode")
	myTestCustomDestination := NewSampleCustomDestination()

	ScrapeInterval = 3
	SyncDeletes = true
	go RunCustomDestinationSync(testClient, &myTestCustomDestination)
	time.Sleep(time.Duration(4) * time.Second) // Give time for sync

	testClient.RegisterSchemaBySubjectAndIDAndVersion(mockSchema, newSubject, 10001, 1, "AVRO", []SchemaReference{})
	time.Sleep(time.Duration(4) * time.Second) // Give time for sync
	_, exists1 := myTestCustomDestination.inMemState[testingSubject]
	_, exists2 := myTestCustomDestination.inMemState[newSubject]

	assert.True(t, exists1)
	assert.True(t, exists2)

	// Test Delete
	testClient.PerformSoftDelete(newSubject, 1)
	testClient.PerformHardDelete(newSubject, 1)
	time.Sleep(time.Duration(4) * time.Second) // Give time for sync

	_, exists3 := myTestCustomDestination.inMemState[newSubject]
	assert.False(t, exists3)

	CancelRun = true
	time.Sleep(time.Duration(4) * time.Second) // Give time for killing goroutine
}
