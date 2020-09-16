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
var hardDeleteLogMessage = "Testing hard delete sync"
var newSchema = "{\"type\":\"record\",\"name\":\"value_newnew\",\"namespace\":\"com.mycorp.mynamespace\",\"doc\":\"Sample schema to help you get started.\",\"fields\":[{\"name\":\"this\",\"type\":\"int\",\"doc\":\"The int type is a 32-bit signed integer.\"},{\"default\": null,\"name\": \"onefield\",\"type\": [\"null\",\"string\"]}]}"
var localRelativePath = "/testingLocalBackupRelativePath"
var localAbsPath = "/tmp/testingLocalBackupAbsPath"

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setup () {
	composeEnv = testcontainers.NewLocalDockerCompose([]string{"docker-compose.yml"},"integrationtests")
	composeEnv.WithCommand([]string{"up","-d"}).Invoke()
	time.Sleep(time.Duration(18) * time.Second) // give services time to set up
	client.ScrapeInterval = 2
	client.SyncDeletes = true
	client.SyncHardDeletes = true
	client.LowerBound = 100000
	client.UpperBound = 101000
	client.HttpCallTimeout = 60

	testClientSrc = client.NewSchemaRegistryClient("http://localhost:8081","testUser", "testPass", "src")
	testClientDst = client.NewSchemaRegistryClient("http://localhost:8082","testUser", "testPass", "dst")

}

func tearDown () {
	composeEnv.WithCommand([]string{"down","-v"}).Invoke()
}

func TestExportMode(t *testing.T) {
	log.Println("Test Export Mode!")

	setImportMode()
	setupSource()

	client.BatchExport(testClientSrc,testClientDst)

	srcChan := make(chan map[string][]int64)
	dstChan := make(chan map[string][]int64)

	go testClientDst.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(dstChan)

	srcSubjects := <- srcChan
	dstSubjects := <- dstChan

	assert.True(t,reflect.DeepEqual(srcSubjects,dstSubjects))

	cleanup()

	log.Println("Test Export Mode with Allow Lists!")
	setupSource()
	client.AllowList = map[string]bool{
		testingSubjectKey: true,
	}
	client.DisallowList = nil

	client.BatchExport(testClientSrc,testClientDst)
	go testClientDst.GetSubjectsWithVersions(dstChan)
	dstSubjects = <- dstChan

	_ , contains := dstSubjects[testingSubjectKey]
	assert.Equal(t, 1, len(dstSubjects))
	assert.True(t, contains)

	cleanup()

	log.Println("Test Export Mode with Disallow Lists!")
	setupSource()

	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	client.BatchExport(testClientSrc,testClientDst)
	go testClientDst.GetSubjectsWithVersions(dstChan)
	dstSubjects = <- dstChan

	_ , contains = dstSubjects[testingSubjectKey]
	assert.Equal(t, 1, len(dstSubjects))
	assert.True(t, contains)

	cleanup()

}

func TestLocalMode(t *testing.T) {
	log.Println("Test Local Mode!")

	setupSource()

	client.DisallowList = nil
	client.AllowList = nil

	assert.True(t,testLocalCopy(6))

	log.Println("Test Local Mode With Allow Lists!")

	client.DisallowList = nil
	client.AllowList = map[string]bool{
		testingSubjectValue: true,
	}

	assert.True(t,testLocalCopy(3))

	log.Println("Test Local Mode With Disallow Lists!")

	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	assert.True(t,testLocalCopy(3))

}


func TestSyncMode(t *testing.T) {
	log.Println("Test Sync Mode!")

	client.CancelRun = false
	client.AllowList = nil
	client.DisallowList = nil

	setImportMode()
	setupSource()

	assert.True(t, commonSyncTest(2,6))

	log.Println("Testing hard delete sync for whole ID")

	aChan := make(chan map[int64]map[string]int64)
	bChan := make(chan map[int64]map[string]int64)
	srcIDs := make(map[int64]map[string]int64)
	dstIDs := make(map[int64]map[string]int64)

	newRegisterKey := client.SchemaRecord{
		Subject: testingSubjectKey,
		Schema:  newSchema,
		SType:   "AVRO",
		Version: 4,
		Id:      100007,
	}

	newRegisterValue := client.SchemaRecord{
		Subject: testingSubjectValue,
		Schema:  newSchema,
		SType:   "AVRO",
		Version: 4,
		Id:      100007,
	}


	testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(newRegisterKey.Schema,
		newRegisterKey.Subject,newRegisterKey.Id,newRegisterKey.Version,newRegisterKey.SType)
	testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(newRegisterValue.Schema,
		newRegisterValue.Subject,newRegisterValue.Id,newRegisterValue.Version,newRegisterValue.SType)
	time.Sleep(time.Duration(11) * time.Second) // Give time for sync

	go testClientSrc.GetAllIDs(aChan)
	go testClientDst.GetAllIDs(bChan)

	srcIDs = <- aChan
	dstIDs = <- bChan
	printIDTestResult(srcIDs, dstIDs)

	// inject a hard delete
	testClientSrc.PerformSoftDelete(testingSubjectValue,4)
	testClientSrc.PerformHardDelete(testingSubjectValue,4)
	time.Sleep(time.Duration(11) * time.Second) // Give time for sync

	go testClientSrc.GetAllIDs(aChan)
	go testClientDst.GetAllIDs(bChan)

	srcIDs = <- aChan
	dstIDs = <- bChan
	printIDTestResult(srcIDs, dstIDs)

	testClientSrc.PerformSoftDelete(testingSubjectKey,4)
	testClientSrc.PerformHardDelete(testingSubjectKey,4)
	time.Sleep(time.Duration(11) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetAllIDs(aChan)
	go testClientDst.GetAllIDs(bChan)
	time.Sleep(time.Duration(6) * time.Second) // Give time for sync

	srcIDs = <- aChan
	dstIDs = <- bChan
	printIDTestResult(srcIDs, dstIDs)

	assert.True(t, reflect.DeepEqual(srcIDs, dstIDs))

	killAsyncRoutine()
	cleanup()

	log.Println("Test Sync Mode With Allow Lists!")
	client.CancelRun = false
	setupSource()

	client.AllowList = map[string]bool{
		testingSubjectKey: true,
	}
	client.DisallowList = nil

	assert.True(t, commonSyncTest(1,3))
	killAsyncRoutine()
	cleanup()

	log.Println("Test Sync Mode With Allow Lists!")
	client.CancelRun = false
	setupSource()

	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	assert.True(t, commonSyncTest(1,3))
	killAsyncRoutine()
	cleanup()

}

/*
Helper methods for integration testing from now on.
 */

func startAsyncRoutine () {
	// Start sync in another goroutine
	go client.Sync(testClientSrc,testClientDst)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync
}

func killAsyncRoutine () {
	log.Println("Killing sync goroutine")
	client.CancelRun = true
	time.Sleep(time.Duration(3) * time.Second) // Give thread time to die
}

func cleanup () {
	log.Println("Clean up SRs")
	testClientDst.DeleteAllSubjectsPermanently()
	testClientSrc.DeleteAllSubjectsPermanently()
	log.Println("Reset source destination SR")
	setupSource()
	time.Sleep(time.Duration(3) * time.Second) // Allow time for deletes to complete
}

func printSubjectTestResult (srcSubjects map[string][]int64, destSubjects map[string][]int64) {
	log.Printf("Source subject-version mapping contents: %v",srcSubjects)
	log.Printf("Destination subject-version mapping contents: %v",destSubjects)
}

func printIDTestResult (srcIDs  map[int64]map[string]int64, dstIDs  map[int64]map[string]int64) {
	log.Printf("Source IDs contents: %v", srcIDs)
	log.Printf("Destination IDs contents: %v", dstIDs)
}

func setupSource () {
	// Set up our source registry
	subjects := []string{testingSubjectValue, testingSubjectKey}
	id := int64(100001)
	versions := []int64{1, 2, 3}

	schema := "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"},{\"name\": \"too\",\"type\": \"string\",\"doc\": \"The string is a unicode character sequence.\"}]}"

	schema2 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"

	schema3 := 	"{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"

	schema4 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"},{\"name\": \"too\",\"type\": \"string\",\"doc\": \"The string is a unicode character sequence.\"}]}"

	schema5 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"

	schema6 := 	"{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"

	schemas := []string{schema, schema2, schema3, schema4, schema5, schema6}
	counter := 1

	for _, subject := range subjects {
		for _ , version := range versions {

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

func setImportMode () {
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

func testSoftDelete (lenOfDestSubjects int) bool {
	log.Println(softDeleteLogMessage)

	// Assert schemas in dest deep equal schemas in src
	srcSubjects := make (map[string][]int64)
	destSubjects := make (map[string][]int64)

	srcChan := make(chan map[string][]int64)
	destChan := make(chan map[string][]int64)

	// inject a soft delete
	testClientSrc.PerformSoftDelete(testingSubjectKey,1)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan
	printSubjectTestResult(srcSubjects, destSubjects)

	return reflect.DeepEqual(srcSubjects, destSubjects) && len(destSubjects) == lenOfDestSubjects
}

func testInitialSync (lenOfDestSubjects int) bool  {
	log.Println("Testing initial sync")
	// Assert schemas in dest deep equal schemas in src
	srcSubjects := make (map[string][]int64)
	destSubjects := make (map[string][]int64)

	srcChan := make(chan map[string][]int64)
	destChan := make(chan map[string][]int64)

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan

	printSubjectTestResult(srcSubjects, destSubjects)

	return reflect.DeepEqual(srcSubjects, destSubjects) && (len(destSubjects) == lenOfDestSubjects)
}

func testRegistrationSync (lenOfDestSubjects int) bool {
	log.Println("Testing registration sync")

	// Assert schemas in dest deep equal schemas in src
	srcSubjects := make (map[string][]int64)
	destSubjects := make (map[string][]int64)

	srcChan := make(chan map[string][]int64)
	destChan := make(chan map[string][]int64)

	newRegister := client.SchemaRecord{
		Subject: testingSubjectKey,
		Schema:  newSchema,
		SType:   "AVRO",
		Version: 4,
		Id:      100007,
	}
	testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(newRegister.Schema,
		newRegister.Subject,newRegister.Id,newRegister.Version,newRegister.SType)

	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan
	printSubjectTestResult(srcSubjects, destSubjects)

	return reflect.DeepEqual(srcSubjects, destSubjects) && (len(destSubjects) == lenOfDestSubjects)
}

func testHardDeleteSync (lenOfDestIDs int) bool {

	log.Println(hardDeleteLogMessage)

	// inject a hard delete
	testClientSrc.PerformHardDelete(testingSubjectKey,1)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	aChan := make(chan map[int64]map[string]int64)
	bChan := make(chan map[int64]map[string]int64)
	srcIDs := make(map[int64]map[string]int64)
	dstIDs := make(map[int64]map[string]int64)

	go testClientSrc.GetAllIDs(aChan)
	go testClientDst.GetAllIDs(bChan)

	srcIDs = <- aChan
	dstIDs = <- bChan
	printIDTestResult(srcIDs, dstIDs)

	return reflect.DeepEqual(srcIDs, dstIDs) && (len(dstIDs) == lenOfDestIDs)
}

func testLocalCopy (expectedFilesToWrite int) bool {

	currentPath, _ := os.Getwd()
	currentPath = filepath.Clean(currentPath)

	// Test Relative Paths
	err := os.Mkdir(currentPath+localRelativePath, 0755)
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(currentPath+localRelativePath)
	client.WriteToFS(testClientSrc, "testingLocalBackupRelativePath", currentPath)

	files, err2 := ioutil.ReadDir(currentPath+localRelativePath)
	if err2 != nil {
		panic(err2)
	}

	// Test Absolute Paths
	_ = os.Mkdir(localAbsPath, 0755)
	defer os.RemoveAll(localAbsPath)
	client.WriteToFS(testClientSrc, localAbsPath, currentPath)

	files2,_ := ioutil.ReadDir(localAbsPath)

	return (len(files2) == expectedFilesToWrite) && (len(files) == expectedFilesToWrite)
}

func commonSyncTest (lenOfDestSubjects int, lenOfDestIDs int ) bool {

	startAsyncRoutine()
	time.Sleep(time.Duration(7) * time.Second) // Give time for sync
	resultInitial :=  testInitialSync(lenOfDestSubjects)
	time.Sleep(time.Duration(7) * time.Second) // Give time for sync
	resultRegistration :=  testRegistrationSync(lenOfDestSubjects)
	time.Sleep(time.Duration(7) * time.Second) // Give time for sync
	resultSoftDelete :=  testSoftDelete(lenOfDestSubjects)
	time.Sleep(time.Duration(7) * time.Second) // Give time for sync
	resultHardDelete :=  testHardDeleteSync(lenOfDestIDs)
	time.Sleep(time.Duration(7) * time.Second) // Give time for sync

	return resultInitial && resultRegistration && resultSoftDelete && resultHardDelete

}