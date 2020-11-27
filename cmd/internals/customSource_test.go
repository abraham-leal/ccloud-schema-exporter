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

	assert.True(t, reflect.DeepEqual(myTestCustomSource.inMemSchemas, subjectState))

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

	assert.True(t, reflect.DeepEqual(map[string][]int64{"test-key": {1}}, subjectState))

	CancelRun = true
	time.Sleep(time.Duration(2) * time.Second) // Give time for killing goroutine

	testClient.DeleteAllSubjectsPermanently()
}
