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
var testingSubject1 = "someSubject-value"
var testingSubject2 = "someSubject-key"

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

	if testClientSrc.IsReachable() && testClientDst.IsReachable() {
		// Set up our source registry
		subjects := []string{testingSubject1,testingSubject2}
		id := int64(100001)
		versions := []int64{1, 2, 3}

		err := testClientSrc.SetMode("IMPORT")
		if err == false {
			log.Println("Could not set source registry to IMPORT ModeRecord.")
			os.Exit(0)
		}

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

	} else {
		panic("Could not find Schema Registries")
	}

	time.Sleep(time.Duration(5) * time.Second) // Allow registration catch up
}

func tearDown () {
	composeEnv.WithCommand([]string{"down","-v"}).Invoke()
}

func TestExportMode(t *testing.T) {
	log.Println("Test Export Mode!")

	if !testClientDst.IsImportModeReady() {
		err := testClientDst.SetMode("IMPORT")
		if err == false {
			log.Println("Could not set destination registry to IMPORT ModeRecord.")
			os.Exit(0)
		}
	}

	client.BatchExport(testClientSrc,testClientDst)

	srcChan := make(chan map[string][]int64)
	dstChan := make(chan map[string][]int64)

	go testClientDst.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(dstChan)

	srcSubjects := <- srcChan
	dstSubjects := <- dstChan

	assert.True(t,reflect.DeepEqual(srcSubjects,dstSubjects))

	testClientDst.DeleteAllSubjectsPermanently()
	time.Sleep(time.Duration(1) * time.Second) // Allow time for deletes to complete
}

func TestLocalMode(t *testing.T) {
	log.Println("Test Local Mode!")
	currentPath, _ := os.Executable()
	relativePathToTest := "/testingLocalBackupRelativePath"
	absPathToTest := "/tmp/testingLocalBackupAbsPath"

	// Test Relative Paths
	err := os.Mkdir(filepath.Dir(currentPath)+relativePathToTest, 0755)
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(filepath.Dir(currentPath)+relativePathToTest)
	client.WriteToFS(testClientSrc, "testingLocalBackupRelativePath")

	files, err2 := ioutil.ReadDir(filepath.Dir(currentPath)+relativePathToTest)
	if err2 != nil {
		panic(err2)
	}
	assert.Equal(t,6,len(files))

	// Test Absolute Paths
	_ = os.Mkdir(absPathToTest, 0755)
	defer os.RemoveAll(absPathToTest)
	client.WriteToFS(testClientSrc, absPathToTest)

	files2,_ := ioutil.ReadDir(absPathToTest)
	assert.Equal(t,6,len(files2))

}


func TestSyncMode(t *testing.T) {
	log.Println("Test Sync Mode!")

	// Set mode
	if !testClientDst.IsImportModeReady() {
		err := testClientDst.SetMode("IMPORT")
		if err == false {
			log.Println("Could not set destination registry to IMPORT ModeRecord.")
			os.Exit(0)
		}
	}

	// Start sync in another goroutine
	go client.Sync(testClientSrc,testClientDst)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src
	srcSubjects := make (map[string][]int64)
	destSubjects := make (map[string][]int64)

	srcChan := make(chan map[string][]int64)
	destChan := make(chan map[string][]int64)

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan

	log.Println("Testing initial sync")
	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))

	// inject a new write
	schema := "{\"type\":\"record\",\"name\":\"value_newnew\",\"namespace\":\"com.mycorp.mynamespace\",\"doc\":\"Sample schema to help you get started.\",\"fields\":[{\"name\":\"this\",\"type\":\"int\",\"doc\":\"The int type is a 32-bit signed integer.\"},{\"default\": null,\"name\": \"onefield\",\"type\": [\"null\",\"string\"]}]}"

	newRegister := client.SchemaRecord{
		Subject: testingSubject1,
		Schema:  schema,
		SType:   "AVRO",
		Version: 4,
		Id:      100007,
	}

	log.Println("Testing registration sync")

	testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(newRegister.Schema,
		newRegister.Subject,newRegister.Id,newRegister.Version,newRegister.SType)

	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan
	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))

	log.Println("Testing soft delete sync")

	// inject a soft delete
	testClientSrc.PerformSoftDelete(testingSubject1,1)
	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan
	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))

	log.Println("Testing hard delete sync for subject")

	// inject a hard delete
	testClientSrc.PerformHardDelete(testingSubject1,1)
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

	assert.True(t, reflect.DeepEqual(srcIDs, dstIDs))

	log.Println("Testing hard delete sync for whole ID")

	newRegister = client.SchemaRecord{
		Subject: testingSubject2,
		Schema:  schema,
		SType:   "AVRO",
		Version: 4,
		Id:      100007,
	}

	testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(newRegister.Schema,
		newRegister.Subject,newRegister.Id,newRegister.Version,newRegister.SType)
	time.Sleep(time.Duration(11) * time.Second) // Give time for sync

	go testClientSrc.GetAllIDs(aChan)
	go testClientDst.GetAllIDs(bChan)

	srcIDs = <- aChan
	dstIDs = <- bChan
	printIDTestResult(srcIDs, dstIDs)

	// inject a hard delete
	testClientSrc.PerformSoftDelete(testingSubject1,4)
	testClientSrc.PerformHardDelete(testingSubject1,4)
	time.Sleep(time.Duration(11) * time.Second) // Give time for sync

	go testClientSrc.GetAllIDs(aChan)
	go testClientDst.GetAllIDs(bChan)

	srcIDs = <- aChan
	dstIDs = <- bChan
	printIDTestResult(srcIDs, dstIDs)


	testClientSrc.PerformSoftDelete(testingSubject2,4)
	testClientSrc.PerformHardDelete(testingSubject2,4)
	time.Sleep(time.Duration(11) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetAllIDs(aChan)
	go testClientDst.GetAllIDs(bChan)

	srcIDs = <- aChan
	dstIDs = <- bChan
	printIDTestResult(srcIDs, dstIDs)

	assert.True(t, reflect.DeepEqual(srcIDs, dstIDs))

	log.Println("Killing sync goroutine")
	client.TestHarnessRun = true
	time.Sleep(time.Duration(3) * time.Second) // Give thread time to die

	log.Println("Clean up destination SR")
	testClientDst.DeleteAllSubjectsPermanently()
	time.Sleep(time.Duration(3) * time.Second) // Allow time for deletes to complete

}

func printSubjectTestResult (srcSubjects map[string][]int64, destSubjects map[string][]int64) {
	log.Printf("Source subject-version mapping contents: %v",srcSubjects)
	log.Printf("Source subject-version mapping contents: %v",destSubjects)
}

func printIDTestResult (srcIDs  map[int64]map[string]int64, dstIDs  map[int64]map[string]int64) {
	log.Printf("Source IDs contents: %v", srcIDs)
	log.Printf("Destination IDs contents: %v", dstIDs)
}
