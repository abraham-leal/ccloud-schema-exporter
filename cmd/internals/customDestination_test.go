package client

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
	testClient.RegisterSchemaBySubjectAndIDAndVersion(mockSchema, testingSubject, 10001, 1, "AVRO")
	myTestCustomDestination := NewSampleCustomDestination()

	RunCustomDestinationBatch(testClient, myTestCustomDestination)

	_, exists := myTestCustomDestination.inMemState[testingSubject]

	assert.True(t, exists)
}

func TCustomDestinationSync(t *testing.T) {
	log.Println("Testing Custom Destination in Sync Mode")
	myTestCustomDestination := NewSampleCustomDestination()

	ScrapeInterval = 3
	go RunCustomDestinationSync(testClient, myTestCustomDestination)
	time.Sleep(time.Duration(4) * time.Second) // Give time for sync

	testClient.RegisterSchemaBySubjectAndIDAndVersion(mockSchema, newSubject, 10001, 1, "AVRO")
	time.Sleep(time.Duration(4) * time.Second) // Give time for sync
	_, exists1 := myTestCustomDestination.inMemState[testingSubject]
	_, exists2 := myTestCustomDestination.inMemState[newSubject]

	assert.True(t, exists1)
	assert.True(t, exists2)

	CancelRun = true
	time.Sleep(time.Duration(4) * time.Second) // Give time for killing goroutine
}
