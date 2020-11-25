package integration

import (
	client "github.com/abraham-leal/ccloud-schema-exporter/cmd/internals"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var composeEnv *testcontainers.LocalDockerCompose
var testClientSrc *client.SchemaRegistryClient
var testClientDst *client.SchemaRegistryClient
var testingSubjectValue = "someSubject-value"
var testingSubjectKey = "someSubject-key"
var softDeleteLogMessage = "Testing soft delete sync"
var newSchema = "{\"type\":\"record\",\"name\":\"value_newnew\",\"namespace\":\"com.mycorp.mynamespace\",\"doc\":\"Sample schema to help you get started.\",\"fields\":[{\"name\":\"this\",\"type\":\"int\",\"doc\":\"The int type is a 32-bit signed integer.\"},{\"default\": null,\"name\": \"onefield\",\"type\": [\"null\",\"string\"]}]}"
var localRelativePath = "/testingLocalBackupRelativePath"
var localAbsPath = "/tmp/testingLocalBackupAbsPath"

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setup() {
	composeEnv = testcontainers.NewLocalDockerCompose([]string{"docker-compose.yml"}, "integrationtests")
	composeEnv.WithCommand([]string{"up", "-d"}).Invoke()
	time.Sleep(time.Duration(18) * time.Second) // give services time to set up
	client.ScrapeInterval = 2
	client.SyncDeletes = true
	client.HttpCallTimeout = 60

	testClientSrc = client.NewSchemaRegistryClient("http://localhost:8081", "testUser", "testPass", "src")
	testClientDst = client.NewSchemaRegistryClient("http://localhost:8082", "testUser", "testPass", "dst")
}

func tearDown() {
	composeEnv.WithCommand([]string{"down", "-v"}).Invoke()
}

func TestExportMode(t *testing.T) {
	log.Println("Test Export Mode!")

	setImportMode()
	setupSource()

	client.BatchExport(testClientSrc, testClientDst)

	srcChan := make(chan map[string][]int64)
	dstChan := make(chan map[string][]int64)

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(dstChan)

	srcSubjects := <-srcChan
	dstSubjects := <-dstChan

	assert.True(t, reflect.DeepEqual(srcSubjects, dstSubjects))

	cleanup()

	log.Println("Test Export Mode with Allow Lists!")
	setupSource()
	client.AllowList = map[string]bool{
		testingSubjectKey: true,
	}
	client.DisallowList = nil

	client.BatchExport(testClientSrc, testClientDst)
	go testClientDst.GetSubjectsWithVersions(dstChan)
	dstSubjects = <-dstChan

	_, contains := dstSubjects[testingSubjectKey]
	assert.Equal(t, 1, len(dstSubjects))
	assert.True(t, contains)

	cleanup()

	log.Println("Test Export Mode with Disallow Lists!")
	setupSource()

	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	client.BatchExport(testClientSrc, testClientDst)
	go testClientDst.GetSubjectsWithVersions(dstChan)
	dstSubjects = <-dstChan

	_, contains = dstSubjects[testingSubjectKey]
	assert.Equal(t, 1, len(dstSubjects))
	assert.True(t, contains)

	cleanup()

}

func TestLocalMode(t *testing.T) {
	log.Println("Test Local Mode!")

	setupSource()

	client.DisallowList = nil
	client.AllowList = nil

	testLocalCopy(t, 6)

	log.Println("Test Local Mode With Allow Lists!")

	client.DisallowList = nil
	client.AllowList = map[string]bool{
		testingSubjectValue: true,
	}

	testLocalCopy(t, 3)

	log.Println("Test Local Mode With Disallow Lists!")

	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	testLocalCopy(t, 3)

}

func TestSyncMode(t *testing.T) {
	log.Println("Test Sync Mode!")

	client.CancelRun = false
	client.AllowList = nil
	client.DisallowList = nil

	setImportMode()
	setupSource()

	commonSyncTest(t, 2)

	killAsyncRoutine()
	cleanup()

	log.Println("Test Sync Mode With Allow Lists!")
	client.CancelRun = false

	client.AllowList = map[string]bool{
		testingSubjectKey: true,
	}
	client.DisallowList = nil

	commonSyncTest(t, 1)
	killAsyncRoutine()
	cleanup()

	log.Println("Test Sync Mode With Disallow Lists!")
	client.CancelRun = false

	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	commonSyncTest(t, 1)
	killAsyncRoutine()
	cleanup()

}

/*
Helper methods for integration testing from now on.
*/

func startAsyncRoutine() {
	// Start sync in another goroutine
	log.Println("Start sync goroutine")
	go client.Sync(testClientSrc, testClientDst)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
}

func killAsyncRoutine() {
	log.Println("Killing sync goroutine")
	client.CancelRun = true
	time.Sleep(time.Duration(3) * time.Second) // Give thread time to die
}

func cleanup() {
	log.Println("Clean up SRs")
	testClientDst.DeleteAllSubjectsPermanently()
	testClientSrc.DeleteAllSubjectsPermanently()
	time.Sleep(time.Duration(3) * time.Second) // Allow time for deletes to complete
	log.Println("Reset source destination SR")
	setupSource()
	time.Sleep(time.Duration(5) * time.Second) // Allow time for deletes to complete
}

func printSubjectTestResult(srcSubjects map[string][]int64, destSubjects map[string][]int64) {
	log.Printf("Source subject-version mapping contents: %v", srcSubjects)
	log.Printf("Destination subject-version mapping contents: %v", destSubjects)
}

func setupSource() {
	// Set up our source registry
	subjects := []string{testingSubjectValue, testingSubjectKey}
	id := int64(100500)
	versions := []int64{1, 2, 3}

	schema := "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"},{\"name\": \"too\",\"type\": \"string\",\"doc\": \"The string is a unicode character sequence.\"}]}"

	schema2 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"

	schema3 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"

	schema4 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"},{\"name\": \"too\",\"type\": \"string\",\"doc\": \"The string is a unicode character sequence.\"}]}"

	schema5 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"

	schema6 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"

	schemas := []string{schema, schema2, schema3, schema4, schema5, schema6}
	counter := 1

	for _, subject := range subjects {
		for _, version := range versions {

			currentRecord := client.SchemaRecord{
				Subject: subject,
				Schema:  schemas[counter-1],
				SType:   "AVRO",
				Version: version,
				Id:      id,
			}

			testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(
				currentRecord.Schema,
				currentRecord.Subject,
				currentRecord.Id,
				currentRecord.Version,
				"AVRO")

			log.Printf("Registering schema with subject %s, version %d, and id %d", currentRecord.Subject, currentRecord.Version, currentRecord.Id)
			id = id + 1
			counter++
		}
	}
}

func setImportMode() {
	if !testClientDst.IsImportModeReady() {
		err := testClientDst.SetMode(client.IMPORT)
		if err == false {
			log.Fatalln("Could not set destination registry to IMPORT ModeRecord.")
		}
	}

	if !testClientSrc.IsImportModeReady() {
		err := testClientSrc.SetMode(client.IMPORT)
		if err == false {
			log.Fatalln("Could not set source registry to IMPORT ModeRecord.")
		}
	}
}

func testSoftDelete(t *testing.T, lenOfDestSubjects int) {
	log.Println(softDeleteLogMessage)

	// Assert schemas in dest deep equal schemas in src
	srcSubjects := make(map[string][]int64)
	destSubjects := make(map[string][]int64)

	srcChan := make(chan map[string][]int64)
	destChan := make(chan map[string][]int64)

	// inject a soft delete
	testClientSrc.PerformSoftDelete(testingSubjectKey, 1)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <-srcChan
	destSubjects = <-destChan
	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))
	assert.True(t, len(destSubjects) == lenOfDestSubjects)

	testClientSrc.PerformHardDelete(testingSubjectKey, 1)
	testClientDst.PerformHardDelete(testingSubjectKey, 1)
}

func testInitialSync(t *testing.T, lenOfDestSubjects int) {
	log.Println("Testing initial sync")
	// Assert schemas in dest deep equal schemas in src
	srcSubjects := make(map[string][]int64)
	destSubjects := make(map[string][]int64)

	srcChan := make(chan map[string][]int64)
	destChan := make(chan map[string][]int64)

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <-srcChan
	destSubjects = <-destChan

	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))
	assert.True(t, len(destSubjects) == lenOfDestSubjects)
}

func testRegistrationSync(t *testing.T, lenOfDestSubjects int) {
	log.Println("Testing registration sync")

	// Assert schemas in dest deep equal schemas in src
	srcSubjects := make(map[string][]int64)
	destSubjects := make(map[string][]int64)

	srcChan := make(chan map[string][]int64)
	destChan := make(chan map[string][]int64)

	newRegister := client.SchemaRecord{
		Subject: testingSubjectValue,
		Schema:  newSchema,
		SType:   "AVRO",
		Version: 4,
		Id:      100507,
	}
	testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(newRegister.Schema,
		newRegister.Subject, newRegister.Id, newRegister.Version, newRegister.SType)

	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <-srcChan
	destSubjects = <-destChan
	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))
	assert.True(t, len(destSubjects) == lenOfDestSubjects)
}

func testLocalCopy(t *testing.T, expectedFilesToWrite int) {

	currentPath, _ := os.Getwd()
	currentPath = filepath.Clean(currentPath)

	// Test Relative Paths
	err := os.Mkdir(currentPath+localRelativePath, 0755)
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(currentPath + localRelativePath)
	client.WriteToFS(testClientSrc, "testingLocalBackupRelativePath", currentPath)

	files, err2 := ioutil.ReadDir(currentPath + localRelativePath)
	if err2 != nil {
		panic(err2)
	}

	// Test Absolute Paths
	_ = os.Mkdir(localAbsPath, 0755)
	defer os.RemoveAll(localAbsPath)
	client.WriteToFS(testClientSrc, localAbsPath, currentPath)

	files2, _ := ioutil.ReadDir(localAbsPath)

	assert.True(t, len(files2) == expectedFilesToWrite)
	assert.True(t, len(files) == expectedFilesToWrite)
}

func commonSyncTest(t *testing.T, lenOfDestSubjects int) {

	startAsyncRoutine()
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
	testInitialSync(t, lenOfDestSubjects)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
	testRegistrationSync(t, lenOfDestSubjects)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
	testSoftDelete(t, lenOfDestSubjects)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

}
