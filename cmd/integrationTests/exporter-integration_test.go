package integration

//
// exporter-integration_test.go
// Copyright 2020 Abraham Leal
//

import (
	"context"
	client "github.com/abraham-leal/ccloud-schema-exporter/cmd/internals"
	testingUtils "github.com/abraham-leal/ccloud-schema-exporter/cmd/testingUtils"
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

var testClientSrc *client.SchemaRegistryClient
var testClientDst *client.SchemaRegistryClient
var cpTestVersion = "7.4.1"
var testingSubjectValue = "someSubject-value"
var testingSubjectKey = "someSubject-key"
var softDeleteLogMessage = "Testing soft delete sync"
var newSchema = "{\"type\":\"record\",\"name\":\"value_newnew\",\"namespace\":\"com.mycorp.mynamespace\",\"doc\":\"Sample schema to help you get started.\",\"fields\":[{\"name\":\"this\",\"type\":\"int\",\"doc\":\"The int type is a 32-bit signed integer.\"},{\"default\": null,\"name\": \"onefield\",\"type\": [\"null\",\"string\"]}]}"
var localRelativePath = "/testingLocalBackupRelativePath"
var localAbsPath = "/tmp/testingLocalBackupAbsPath"
var registrationCount = 0
var registeredSubjectCount = 0
var seenSubjects map[string]string

// Schemas for testing
var schema = "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"},{\"name\": \"too\",\"type\": \"string\",\"doc\": \"The string is a unicode character sequence.\"}]}"
var schema2 = "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"
var schema3 = "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"
var schema4 = "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"},{\"name\": \"too\",\"type\": \"string\",\"doc\": \"The string is a unicode character sequence.\"}]}"
var schema5 = "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"
var schema6 = "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"
var schemaReferenceForSchemaLoad = "{\"type\": \"record\",\"namespace\": \"com.mycorp.schemaLoad\",\"name\": \"value_reference\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\",\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"
var schemaReferenceForSchemaLoadEvolved = "{\"type\": \"record\",\"namespace\": \"com.mycorp.schemaLoad\",\"name\": \"value_reference\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"
var schemaReferencerSchemaLoad = "{\"type\": \"record\",\"namespace\": \"com.mycorp.schemaLoad\",\"name\": \"value_referencing\",\"doc\": \"Sample schema to help you get started.\",\"fields\": [{\"name\": \"this\",\"type\":\"com.mycorp.schemaLoad.value_reference\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"
var schemaToReference = "{\"type\":\"record\",\"name\":\"reference\",\"namespace\":\"com.reference\",\"fields\":[{\"name\":\"someField\",\"type\":\"string\"},{\"name\":\"someField2\",\"type\":\"int\"}]}"
var schemaToReferenceFinal = "{\"type\":\"record\",\"name\":\"referenceWithDepth\",\"namespace\":\"com.reference\",\"fields\":[{\"name\":\"someField\",\"type\":\"string\"}]}"
var schemaReferencing = "{\"type\":\"record\",\"name\":\"sampleRecordreferencing\",\"namespace\":\"com.mycorp.somethinghere\",\"fields\":[{\"name\":\"reference\",\"type\":\"com.reference.reference\"}]}"
var localKafkaContainer testcontainers.Container
var localSchemaRegistrySrcContainer testcontainers.Container
var localSchemaRegistryDstContainer testcontainers.Container
var ctx = context.Background()

/*
	General Integration Testing Framework for the ccloud-schema-exporter.
	To register more schemas for testing, add to setupSource() and make sure to use registerAtSource to register the
	schema in the source Schema Registry.
*/

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setup() {
	networkName := "integration"

	localKafkaContainer, localSchemaRegistrySrcContainer = testingUtils.GetBaseInfra(networkName)
	err := error(nil)
	localSchemaRegistryDstContainer, err = testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "confluentinc/cp-schema-registry:" + cpTestVersion,
				ExposedPorts: []string{"8082/tcp"},
				WaitingFor:   testingUtils.GetSRWaitStrategy("8082"),
				Name:         "schema-registry-dst-" + networkName,
				Env: map[string]string{
					"SCHEMA_REGISTRY_HOST_NAME":                           "schema-registry-dst",
					"SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID":            "schema-dst",
					"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS":        "broker" + networkName + ":29092",
					"SCHEMA_REGISTRY_KAFKASTORE_TOPIC":                    "_schemas-dst",
					"SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR": "1",
					"SCHEMA_REGISTRY_MODE_MUTABILITY":                     "true",
					"SCHEMA_REGISTRY_LISTENERS":                           "http://0.0.0.0:8082",
				},
				Networks: []string{networkName},
			},
			Started: true,
		})
	testingUtils.CheckFail(err, "Dst SR was not able to start")

	client.ScrapeInterval = 2
	client.SyncDeletes = true
	client.HttpCallTimeout = 60

	srcSRPort, err := localSchemaRegistrySrcContainer.MappedPort(ctx, "8081")
	testingUtils.CheckFail(err, "Not Able to get SRC SR Port")
	dstSRPort, err := localSchemaRegistryDstContainer.MappedPort(ctx, "8082")
	testingUtils.CheckFail(err, "Not Able to get DST SR Port")

	testClientSrc = client.NewSchemaRegistryClient("http://localhost:"+srcSRPort.Port(), "testUser", "testPass", "src")
	testClientDst = client.NewSchemaRegistryClient("http://localhost:"+dstSRPort.Port(), "testUser", "testPass", "dst")
}

func tearDown() {
	err := localSchemaRegistrySrcContainer.Terminate(ctx)
	testingUtils.CheckFail(err, "Could not terminate source sr")
	err = localSchemaRegistryDstContainer.Terminate(ctx)
	testingUtils.CheckFail(err, "Could not terminate destination sr")
	err = localKafkaContainer.Terminate(ctx)
	testingUtils.CheckFail(err, "Could not terminate kafka")
}

func TestSchemaLoad(t *testing.T) {
	log.Println("Testing Schema Load: AVRO!")

	testClientDst.SetMode(client.READWRITE)

	testSchemaLoadAvro(t, 9)
}

func TestExportMode(t *testing.T) {
	log.Println("Test Export Mode!")

	setImportMode()
	cleanup()

	client.BatchExport(testClientSrc, testClientDst)

	srcChan := make(chan map[string][]int64)
	dstChan := make(chan map[string][]int64)

	go testClientSrc.GetSubjectsWithVersions(srcChan, false)
	go testClientDst.GetSubjectsWithVersions(dstChan, false)

	srcSubjects := <-srcChan
	dstSubjects := <-dstChan

	assert.True(t, reflect.DeepEqual(srcSubjects, dstSubjects))

	cleanup()

	log.Println("Test Export Mode with Allow Lists!")
	client.AllowList = map[string]bool{
		testingSubjectKey: true,
	}
	client.DisallowList = nil

	client.BatchExport(testClientSrc, testClientDst)
	go testClientDst.GetSubjectsWithVersions(dstChan, false)
	dstSubjects = <-dstChan

	_, contains := dstSubjects[testingSubjectKey]
	assert.Equal(t, len(client.AllowList), len(dstSubjects))
	assert.True(t, contains)

	cleanup()

	log.Println("Test Export Mode with Disallow Lists!")

	//Get length of subjects without filters
	client.AllowList = nil
	client.DisallowList = nil
	go testClientSrc.GetSubjectsWithVersions(dstChan, false)
	dstSubjectsAll := <-dstChan

	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	client.BatchExport(testClientSrc, testClientDst)
	go testClientDst.GetSubjectsWithVersions(dstChan, false)
	dstSubjects = <-dstChan

	_, contains = dstSubjects[testingSubjectKey]
	log.Println(len(dstSubjectsAll))
	log.Println(len(client.DisallowList))
	assert.Equal(t, len(dstSubjectsAll)-len(client.DisallowList), len(dstSubjects))
	assert.True(t, contains)
}

func TestLocalMode(t *testing.T) {
	log.Println("Test Local Mode!")

	setImportMode()
	cleanup()

	client.DisallowList = nil
	client.AllowList = nil

	// 3 versions + 3 versions + 1 version referenced + 1 version referencer
	testLocalCopy(t, registrationCount)

	log.Println("Test Local Mode With Allow Lists!")
	cleanup()

	client.DisallowList = nil
	client.AllowList = map[string]bool{
		testingSubjectValue: true,
	}

	dstChan := make(chan map[string][]int64)
	go testClientSrc.GetSubjectsWithVersions(dstChan, false)
	dstSubjects := <-dstChan
	log.Println(dstSubjects)

	filteredSubjectCount := 0
	for _, versions := range dstSubjects {
		for _, _ = range versions {
			filteredSubjectCount++
		}
	}

	// 3 versions
	testLocalCopy(t, filteredSubjectCount)

	log.Println("Test Local Mode With Disallow Lists!")
	cleanup()

	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	go testClientSrc.GetSubjectsWithVersions(dstChan, false)
	dstSubjects = <-dstChan
	log.Println(dstSubjects)

	filteredSubjectCount = 0
	for _, versions := range dstSubjects {
		for _, _ = range versions {
			filteredSubjectCount++
		}
	}

	// 3 versions + 1 version referenced + 1 version referencer
	testLocalCopy(t, filteredSubjectCount)
}

func TestSyncMode(t *testing.T) {
	log.Println("Test Sync Mode!")

	client.CancelRun = false
	client.AllowList = nil
	client.DisallowList = nil

	setImportMode()
	cleanup()

	commonSyncTest(t, registeredSubjectCount)

	killAsyncRoutine()
	cleanup()

	log.Println("Test Sync Mode With Allow Lists!")
	client.CancelRun = false

	client.AllowList = map[string]bool{
		testingSubjectKey: true,
	}
	client.DisallowList = nil

	commonSyncTest(t, len(client.AllowList))
	killAsyncRoutine()

	cleanup()

	log.Println("Test Sync Mode With Disallow Lists!")
	client.CancelRun = false

	// Filter the test
	client.AllowList = nil
	client.DisallowList = map[string]bool{
		testingSubjectValue: true,
	}

	commonSyncTest(t, registeredSubjectCount-len(client.DisallowList))
	killAsyncRoutine()
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
	registeredSubjectCount = 0
	registrationCount = 0
	seenSubjects = map[string]string{}

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

			registerAtSource(
				currentRecord.Schema,
				currentRecord.Subject,
				currentRecord.Id,
				currentRecord.Version,
				"AVRO",
				nil)

			id = id + 1
			counter++
		}
	}

	// One More depth level
	registerAtSource(schemaToReferenceFinal, "referenceWithDepth", 12344, 1, "AVRO", nil)
	referenceStructDepth := client.SchemaReference{
		Name:    "com.reference.referenceWithDepth",
		Subject: "referenceWithDepth",
		Version: 1,
	}
	// Register referencing test
	registerAtSource(schemaToReference, "reference", 12345, 1, "AVRO", []client.SchemaReference{referenceStructDepth})
	referenceStruct := client.SchemaReference{
		Name:    "com.reference.reference",
		Subject: "reference",
		Version: 1,
	}
	registerAtSource(schemaReferencing, "someReferencingSubject", 12346, 1, "AVRO", []client.SchemaReference{referenceStruct})
}

func registerAtSource(schema string, subject string, id int64, version int64, SType string, references []client.SchemaReference) {
	log.Printf("Registering schema with subject %s, version %d, and id %d", subject, version, id)
	testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(schema, subject, id, version, SType, references)
	registrationCount++
	_, contains := seenSubjects[subject]
	if !contains {
		seenSubjects[subject] = ""
		registeredSubjectCount++
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

	go testClientSrc.GetSubjectsWithVersions(srcChan, false)
	go testClientDst.GetSubjectsWithVersions(destChan, false)

	srcSubjects = <-srcChan
	destSubjects = <-destChan
	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))
	assert.Equal(t, lenOfDestSubjects, len(destSubjects))

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

	go testClientSrc.GetSubjectsWithVersions(srcChan, false)
	go testClientDst.GetSubjectsWithVersions(destChan, false)

	srcSubjects = <-srcChan
	destSubjects = <-destChan

	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))
	assert.Equal(t, lenOfDestSubjects, len(destSubjects))
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
	registerAtSource(newRegister.Schema,
		newRegister.Subject, newRegister.Id, newRegister.Version, newRegister.SType, nil)

	time.Sleep(time.Duration(10) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	go testClientSrc.GetSubjectsWithVersions(srcChan, false)
	go testClientDst.GetSubjectsWithVersions(destChan, false)

	srcSubjects = <-srcChan
	destSubjects = <-destChan
	printSubjectTestResult(srcSubjects, destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))
	assert.Equal(t, lenOfDestSubjects, len(destSubjects))
}

func testLocalCopy(t *testing.T, expectedFilesToWrite int) {

	currentPath, _ := os.Getwd()
	currentPath = filepath.Clean(currentPath)
	testClientDst.DeleteAllSubjectsPermanently()

	// Test Relative Paths
	err := os.Mkdir(currentPath+localRelativePath, 0755)
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(currentPath + localRelativePath)
	client.WriteToFS(testClientSrc, "testingLocalBackupRelativePath", currentPath)
	client.WriteFromFS(testClientDst, "testingLocalBackupRelativePath", currentPath)

	files, err2 := ioutil.ReadDir(currentPath + localRelativePath)
	if err2 != nil {
		panic(err2)
	}

	dstSubj := client.GetCurrentSubjectState(testClientDst)

	count := 0
	// Get total schema count
	for _, versions := range dstSubj {
		for _, _ = range versions {
			count = count + 1
		}
	}

	testClientDst.DeleteAllSubjectsPermanently()

	assert.Equal(t, expectedFilesToWrite, count)
	assert.Equal(t, expectedFilesToWrite, len(files))

	// Test Absolute Paths
	_ = os.Mkdir(localAbsPath, 0755)
	defer os.RemoveAll(localAbsPath)
	client.WriteToFS(testClientSrc, localAbsPath, currentPath)
	client.WriteFromFS(testClientDst, localAbsPath, currentPath)

	dstSubj = client.GetCurrentSubjectState(testClientDst)

	count = 0
	// Get total schema count
	for _, versions := range dstSubj {
		for _, _ = range versions {
			count = count + 1
		}
	}

	testClientDst.DeleteAllSubjectsPermanently()

	files2, _ := ioutil.ReadDir(localAbsPath)

	assert.Equal(t, expectedFilesToWrite, count)
	assert.Equal(t, expectedFilesToWrite, len(files2))

}

func testSchemaLoadAvro(t *testing.T, expectedLoadNumber int) {

	currentPath, _ := os.Getwd()
	currentPath = filepath.Clean(currentPath)
	testClientDst.DeleteAllSubjectsPermanently()

	// Test Relative Paths
	err := os.Mkdir(currentPath+localRelativePath, 0755)
	if err != nil {
		panic(err)
	}

	// Write files for Schema Load testing
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad1", schemaReferencerSchemaLoad)
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad2", schemaReferenceForSchemaLoad)
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad3", schemaReferenceForSchemaLoadEvolved)
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad4", schema)
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad5", schema2)
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad6", schema3)
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad7", schema4)
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad8", schema5)
	client.WriteFile(currentPath+localRelativePath, "someSchemaLoad9", schema6)
	defer os.RemoveAll(currentPath + localRelativePath)

	avroLoader := client.NewSchemaLoader(client.AVRO.String(), testClientDst, "testingLocalBackupRelativePath", currentPath)
	avroLoader.Run()

	dstSubj := client.GetCurrentSubjectState(testClientDst)

	count := 0
	// Get total schema count
	for _, versions := range dstSubj {
		for _, _ = range versions {
			count = count + 1
		}
	}

	ReferencingSchemaReferences := testClientDst.GetSchema("com.mycorp.schemaLoad.value_referencing-value", 1, false).References
	versionOfReferencedSchema := ReferencingSchemaReferences[0].Version

	testClientDst.DeleteAllSubjectsPermanently()
	numOfFilesWritten, _ := ioutil.ReadDir(currentPath + localRelativePath)

	assert.Equal(t, expectedLoadNumber, count)
	assert.Equal(t, expectedLoadNumber, len(numOfFilesWritten))
	assert.Equal(t, int64(2), versionOfReferencedSchema)
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
