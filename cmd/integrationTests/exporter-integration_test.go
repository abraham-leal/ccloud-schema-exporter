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

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setup () {
	composeEnv = testcontainers.NewLocalDockerCompose([]string{"docker-compose.yml"},"integrationtests")
	composeEnv.WithCommand([]string{"up","-d"}).Invoke()
	time.Sleep(time.Duration(15) * time.Second) // give services time to set up
	client.ScrapeInterval = 3
	client.SyncDeletes = true

	testClientSrc = client.NewSchemaRegistryClient("http://localhost:8081","testUser", "testPass", "src")
	testClientDst = client.NewSchemaRegistryClient("http://localhost:8082","testUser", "testPass", "dst")

	if testClientSrc.IsReachable() && testClientDst.IsReachable() {
		// Set up our source registry
		subjects := []string{"someSubject-value","someSubject-key"}
		id := 1
		versions := []int{1, 2, 3}

		err := testClientSrc.SetMode("IMPORT")
		if err == false {
			log.Println("Could not set source registry to IMPORT ModeRecord.")
			os.Exit(0)
		}

		schema := "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\"," +
			"\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\"," +
			"\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"},{\"name\": \"too\",\"type\": \"string\",\"doc\": \"The string is a unicode character sequence.\"}]}"

		schema2 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\"," +
			"\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\"," +
			"\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"

		schema3 := 	"{\"type\": \"record\",\"namespace\": \"com.mycorp.mynamespace\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\"," +
			"\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"

		schema4 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\"," +
			"\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\"," +
			"\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"},{\"name\": \"too\",\"type\": \"string\",\"doc\": \"The string is a unicode character sequence.\"}]}"

		schema5 := "{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\"," +
			"\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"},{\"name\": \"that\",\"type\": \"double\"," +
			"\"doc\": \"The double type is a double precision (64-bit) IEEE 754 floating-point number.\"}]}"

		schema6 := 	"{\"type\": \"record\",\"namespace\": \"com.mycorp.wassup\",\"name\": \"value_newnew\",\"doc\": \"Sample schema to help you get started.\"," +
			"\"fields\": [{\"name\": \"this\",\"type\":\"int\",\"doc\": \"The int type is a 32-bit signed integer.\"}]}"

		schemas := []string{schema, schema2, schema3, schema4, schema5, schema6}

		for _, subject := range subjects {
			for _ , version := range versions {

				currentRecord := client.SchemaRecord{
					Subject: subject,
					Schema:  schemas[id-1],
					SType:   "AVRO",
					Version: int64(version),
					Id:      int64(id),
				}

				testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(
					currentRecord.Schema,
					currentRecord.Subject,
					int(currentRecord.Id),
					int(currentRecord.Version),
					"AVRO")

				log.Printf("Registering schema with subject %s, version %d, and id %d", currentRecord.Subject, currentRecord.Version, currentRecord.Id)
				id = id + 1
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

	err := testClientDst.SetMode("IMPORT")
	if err == false {
		log.Println("Could not set destination registry to IMPORT ModeRecord.")
		os.Exit(0)
	}

	client.BatchExport(testClientSrc,testClientDst)

	srcChan := make(chan map[string][]int)
	go testClientDst.GetSubjectsWithVersions(srcChan)
	srcSubjects := <- srcChan

	dstChan := make(chan map[string][]int)
	go testClientDst.GetSubjectsWithVersions(dstChan)
	dstSubjects := <- dstChan

	assert.True(t,reflect.DeepEqual(srcSubjects,dstSubjects))
}

func TestLocalMode(t *testing.T) {
	currentPath, _ := os.Executable()

	// Test Relative Paths
	err := os.Mkdir(filepath.Dir(currentPath)+"/testingLocalBackupRelativePath", 0755)
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(filepath.Dir(currentPath)+"/testingLocalBackupRelativePath")
	client.WriteToFS(testClientSrc, "testingLocalBackupRelativePath")

	files, err2 := ioutil.ReadDir(filepath.Dir(currentPath)+"/testingLocalBackupRelativePath")
	if err2 != nil {
		panic(err2)
	}
	assert.Equal(t,6,len(files))

	// Test Absolute Paths
	_ = os.Mkdir("/tmp/testingLocalBackupAbsPath", 0755)
	defer os.RemoveAll("/tmp/testingLocalBackupAbsPath")
	client.WriteToFS(testClientSrc, "/tmp/testingLocalBackupAbsPath")

	files2,_ := ioutil.ReadDir("/tmp/testingLocalBackupAbsPath")
	assert.Equal(t,6,len(files2))

}


func TestSyncMode(t *testing.T) {

	// Set mode
	err := testClientDst.SetMode("IMPORT")
	if err == false {
		log.Println("Could not set destination registry to IMPORT ModeRecord.")
		os.Exit(0)
	}

	// Start sync in another goroutine
	go client.Sync(testClientSrc,testClientDst)
	time.Sleep(time.Duration(5) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src
	srcSubjects := make (map[string][]int)
	destSubjects := make (map[string][]int)

	srcChan := make(chan map[string][]int)
	destChan := make(chan map[string][]int)

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan

	log.Printf("Source: %v", srcSubjects)
	log.Printf("Destination: %v", destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))

	// inject a new write
	schema := "{\"type\":\"record\",\"name\":\"value_newnew\",\"namespace\":\"com.mycorp.mynamespace\",\"doc\":\"Sample schema to help you get started.\",\"fields\":[{\"name\":\"this\",\"type\":\"int\",\"doc\":\"The int type is a 32-bit signed integer.\"},{\"default\": null,\"name\": \"onefield\",\"type\": [\"null\",\"string\"]}]}"

	newRegister := client.SchemaRecord{
		Subject: "someSubject-value",
		Schema:  schema,
		SType:   "AVRO",
		Version: 4,
		Id:      7,
	}
	testClientSrc.RegisterSchemaBySubjectAndIDAndVersion(newRegister.Schema,
		newRegister.Subject,int(newRegister.Id),int(newRegister.Version),newRegister.SType)
	time.Sleep(time.Duration(5) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src
	srcSubjects = make (map[string][]int)
	destSubjects = make (map[string][]int)

	srcChan = make(chan map[string][]int)
	destChan = make(chan map[string][]int)


	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan

	log.Printf("Source: %v", srcSubjects)
	log.Printf("Destination: %v", destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))

	// inject a soft delete
	testClientSrc.PerformSoftDelete("someSubject-value",1)
	time.Sleep(time.Duration(5) * time.Second) // Give time for sync

	// Assert schemas in dest deep equal schemas in src

	srcSubjects = make (map[string][]int)
	destSubjects = make (map[string][]int)

	srcChan = make(chan map[string][]int)
	destChan = make(chan map[string][]int)

	go testClientSrc.GetSubjectsWithVersions(srcChan)
	go testClientDst.GetSubjectsWithVersions(destChan)

	srcSubjects = <- srcChan
	destSubjects = <- destChan

	log.Printf("Source: %v", srcSubjects)
	log.Printf("Destination: %v", destSubjects)

	assert.True(t, reflect.DeepEqual(srcSubjects, destSubjects))

	log.Println("Killing sync goroutine")
	client.TestHarnessRun = true
	time.Sleep(time.Duration(3) * time.Second) // Give thread time to die

}
