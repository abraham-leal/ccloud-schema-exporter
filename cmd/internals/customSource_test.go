package client

import (
	"github.com/stretchr/testify/assert"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestMainStackCustomSource(t *testing.T) {
	setup()
	t.Run("TCustomSourceBatch", func(t *testing.T) { TCustomSourceBatch(t) })
	t.Run("TCustomSourceSync", func(t *testing.T) { TCustomSourceSync(t) })
	tearDown()
}

func TCustomSourceBatch(t *testing.T) {
	log.Println("Testing Custom Source in Batch Mode")

	schema1 := SchemaRecord{
		Subject: testingSubject,
		Schema:  mockSchema,
		SType:   "AVRO",
		Version: 1,
		Id:      10001,
	}

	myTestCustomSource := NewInMemRegistry([]SchemaRecord{schema1})

	RunCustomSourceBatch(testClient, myTestCustomSource)

	subjectState := GetCurrentSubjectState(testClient)
	log.Println(subjectState)
	log.Println(myTestCustomSource.inMemSchemas)

	assert.True(t,  reflect.DeepEqual(subjectState[myTestCustomSource.inMemSchemas[10001].Subject], []int64{1}))

	testClient.DeleteAllSubjectsPermanently()
}

func TCustomSourceSync(t *testing.T) {
	log.Println("Testing Custom Source in Sync Mode")
	schema1 := SchemaRecord{
		Subject: testingSubject,
		Schema:  mockSchema,
		SType:   "AVRO",
		Version: 1,
		Id:      10001,
	}

	myTestCustomSource := NewInMemRegistry([]SchemaRecord{schema1})

	ScrapeInterval = 3
	go RunCustomSourceSync(testClient, myTestCustomSource)
	time.Sleep(time.Duration(4) * time.Second) // Give time for sync

	subjectState := GetCurrentSubjectState(testClient)
	log.Println(subjectState)
	log.Println(myTestCustomSource.inMemSchemas)

	assert.True(t,  reflect.DeepEqual(subjectState[myTestCustomSource.inMemSchemas[10001].Subject], []int64{1}))

	CancelRun = true
	time.Sleep(time.Duration(2) * time.Second) // Give time for killing goroutine

	testClient.DeleteAllSubjectsPermanently()
}
