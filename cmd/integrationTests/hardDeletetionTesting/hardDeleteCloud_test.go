package integration_deletion

//
// hardDeleteCloud_test.go
// Copyright 2020 Abraham Leal
//


import (
	client "github.com/abraham-leal/ccloud-schema-exporter/cmd/internals"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"reflect"
	"testing"
	"time"
)

var testClientSrc *client.SchemaRegistryClient
var testClientDst *client.SchemaRegistryClient
var testingSubjectKey = "thisCannotExist-key"
var newSchema = "{\"type\":\"record\",\"name\":\"value_newnew\",\"namespace\":\"com.mycorp.mynamespace\",\"doc\":\"Sample schema to help you get started.\",\"fields\":[{\"name\":\"this\",\"type\":\"int\",\"doc\":\"The int type is a 32-bit signed integer.\"},{\"default\": null,\"name\": \"onefield\",\"type\": [\"null\",\"string\"]}]}"

func TestMain(m *testing.M) {
	setupHardDeletesTest()
	code := m.Run()
	os.Exit(code)
}

func setupHardDeletesTest() {
	client.ScrapeInterval = 2
	client.SyncDeletes = true
	client.SyncHardDeletes = true
	client.HttpCallTimeout = 60

	testClientSrc = client.NewSchemaRegistryClient(os.Getenv("XX_SRC_CCLOUD_URL"), os.Getenv("XX_SRC_CCLOUD_KEY"), os.Getenv("XX_SRC_CCLOUD_SECRET"), "src")
	testClientDst = client.NewSchemaRegistryClient(os.Getenv("XX_DST_CCLOUD_URL"), os.Getenv("XX_DST_CCLOUD_KEY"), os.Getenv("XX_DST_CCLOUD_SECRET"), "dst")
}

func TestSyncModeHardDeletes(t *testing.T) {
	log.Println("Test Sync Hard Delete Mode!")

	setupHardDeletesTest()
	client.CancelRun = false
	client.AllowList = nil
	client.DisallowList = nil

	setImportMode()

	testClientSrc.RegisterSchema(newSchema, testingSubjectKey, "AVRO")

	client.AllowList = map[string]bool{
		testingSubjectKey: true,
	}
	client.DisallowList = nil

	commonSyncTest(t)
	killAsyncRoutine()
	cleanup()

}

func setImportMode() {
	if !testClientDst.IsImportModeReady() {
		err := testClientDst.SetMode(client.IMPORT)
		if err == false {
			log.Fatalln("Could not set destination registry to IMPORT ModeRecord.")
		}
	}
}

func commonSyncTest(t *testing.T) {

	startAsyncRoutine()
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
	testInitialSync(t, 1)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
	testSoftDelete(t, 0)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
	testHardDeleteSync(t, 0)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
}

func startAsyncRoutine() {
	// Start sync in another goroutine
	go client.Sync(testClientSrc, testClientDst)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
}

func killAsyncRoutine() {
	log.Println("Killing async goroutine")
	client.CancelRun = true
	time.Sleep(time.Duration(3) * time.Second) // Give thread time to die
}

func cleanup() {
	log.Println("Clean up SR")
	testClientDst.DeleteAllSubjectsPermanently()
	time.Sleep(time.Duration(3) * time.Second) // Allow time for deletes to complete
}

func printSubjectTestResult(srcSubjects map[string][]int64, destSubjects map[string][]int64) {
	log.Printf("Source subject-version mapping contents: %v", srcSubjects)
	log.Printf("Destination subject-version mapping contents: %v", destSubjects)
}

func printIDTestResult(srcIDs map[int64]map[string][]int64, dstIDs map[int64]map[string][]int64) {
	log.Printf("Source IDs contents: %v", srcIDs)
	log.Printf("Destination IDs contents: %v", dstIDs)
}

func testSoftDelete(t *testing.T, lenOfDestSubjects int) {

	// inject a soft delete
	testClientSrc.PerformSoftDelete(testingSubjectKey, 1)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	srcSubjects, destSubjects := getCurrentState()
	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))
	assert.True(t, len(destSubjects) == lenOfDestSubjects)
}

func testInitialSync(t *testing.T, lenOfDestSubjects int) {
	log.Println("Testing initial sync")
	// Assert schemas in dest deep equal schemas in src

	srcSubjects, destSubjects := getCurrentState()

	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))
	assert.True(t, len(destSubjects) == lenOfDestSubjects)
}

func testHardDeleteSync(t *testing.T, lenOfDestIDs int) {

	// inject a hard delete
	testClientSrc.PerformHardDelete(testingSubjectKey, 1)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src
	srcIDs := testClientSrc.GetSoftDeletedIDs()
	dstIDs := testClientDst.GetSoftDeletedIDs()

	printIDTestResult(srcIDs, dstIDs)

	assert.True(t, reflect.DeepEqual(srcIDs, dstIDs))
	assert.True(t, len(dstIDs) == lenOfDestIDs)

}

func getCurrentState() (map[string][]int64, map[string][]int64) {

	srcSubjects := make(map[string][]int64)
	destSubjects := make(map[string][]int64)

	srcChan := make(chan map[string][]int64)
	destChan := make(chan map[string][]int64)

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <-srcChan
	destSubjects = <-destChan

	return srcSubjects, destSubjects
}
