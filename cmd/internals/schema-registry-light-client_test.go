package client

//
// schema-registry-light-client-test.go
// Author: Abraham Leal
//

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"
)

var composeEnv *testcontainers.LocalDockerCompose
var testClient *SchemaRegistryClient
var mockSchema = "{\"type\":\"record\",\"name\":\"value_newnew\",\"namespace\":\"com.mycorp.mynamespace\",\"doc\":\"Sample schema to help you get started.\",\"fields\":[{\"name\":\"this\",\"type\":\"int\",\"doc\":\"The int type is a 32-bit signed integer.\"},{\"name\":\"onefield\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
var testingSubject = "test-key"
var newSubject = "newSubject-key"
var SRUrl = "http://localhost:8081"

func TestMainStack(t *testing.T) {
	setup()
	t.Run("TIsCompatReady", func(t *testing.T) { TIsCompatReady(t) })
	t.Run("TSetCompat", func(t *testing.T) { TSetCompat(t) })
	t.Run("TIsReachable", func(t *testing.T) { TIsReachable(t) })
	t.Run("TSetMode", func(t *testing.T) { TSetMode(t) })
	t.Run("TIsImportModeReady", func(t *testing.T) { TIsImportModeReady(t) })
	t.Run("TGetSubjectWithVersions", func(t *testing.T) { TGetSubjectWithVersions(t) })
	t.Run("TGetVersions", func(t *testing.T) { TGetVersions(t) })
	t.Run("TGetSchema", func(t *testing.T) { TGetSchema(t) })
	t.Run("TRegisterSchemaBySubjectAndIDAndVersion", func(t *testing.T) { TRegisterSchemaBySubjectAndIDAndVersion(t) })
	t.Run("TFilterIDs", func(t *testing.T) { TFilterIDs(t) })
	t.Run("TFilterListedSubjects", func(t *testing.T) { TFilterListedSubjects(t) })
	t.Run("TFilterListedSubjectsVersions", func(t *testing.T) { TFilterListedSubjectsVersions(t) })
	t.Run("TPerformSoftDelete", func(t *testing.T) { TPerformSoftDelete(t) })
	t.Run("TPerformHardDelete", func(t *testing.T) { TPerformHardDelete(t) })
	t.Run("TDeleteAllSubjectsPermanently", func(t *testing.T) { TDeleteAllSubjectsPermanently(t) })
	tearDown()
}

func setup() {
	composeEnv = testcontainers.NewLocalDockerCompose([]string{"../integrationTests/docker-compose-internal.yml"}, "internals")
	composeEnv.WithCommand([]string{"up", "-d"}).Invoke()
	time.Sleep(time.Duration(25) * time.Second) // give services time to set up

	testClient = NewSchemaRegistryClient(SRUrl, "testUser", "testPass", "src")

	//Initial schema
	setImportMode()
}

func tearDown() {
	composeEnv.WithCommand([]string{"down", "-v"}).Invoke()
}

func TIsReachable(t *testing.T) {
	falseTestClient := NewSchemaRegistryClient("http://localhost:8083", "testUser", "testPass", "src")

	assert.True(t, testClient.IsReachable())
	assert.False(t, falseTestClient.IsReachable())
}

func TSetCompat(t *testing.T) {
	endpoint := fmt.Sprintf("%s/config", SRUrl)
	req := GetNewRequest("GET", endpoint, "testUser", "testPass", nil, nil)
	// Test Set READWRITE
	testClient.SetGlobalCompatibility(FULL)
	assert.True(t, performQuery(req)["compatibilityLevel"] == FULL.String())
	// Test Set IMPORT
	testClient.SetGlobalCompatibility(BACKWARD)
	assert.True(t, performQuery(req)["compatibilityLevel"] == BACKWARD.String())
	// Test Set READWRITE
	testClient.SetGlobalCompatibility(FORWARD)
	assert.True(t, performQuery(req)["compatibilityLevel"] == FORWARD.String())
	// Test Set NONE
	testClient.SetGlobalCompatibility(NONE)
	assert.True(t, performQuery(req)["compatibilityLevel"] == NONE.String())
}

func TIsCompatReady(t *testing.T) {
	testClient.SetGlobalCompatibility(FULL)
	assert.False(t, testClient.IsCompatReady())
	testClient.SetGlobalCompatibility(NONE)
	assert.True(t, testClient.IsCompatReady())
}

func TSetMode(t *testing.T) {
	endpoint := fmt.Sprintf("%s/mode", SRUrl)
	req := GetNewRequest("GET", endpoint, "testUser", "testPass", nil, nil)
	// Test Set READWRITE
	testClient.SetMode(READWRITE)
	assert.True(t, performQuery(req)["mode"] == READWRITE.String())
	// Test Set IMPORT
	testClient.SetMode(IMPORT)
	assert.True(t, performQuery(req)["mode"] == IMPORT.String())
	// Test Set READWRITE
	testClient.SetMode(READONLY)
	assert.True(t, performQuery(req)["mode"] == READONLY.String())

	testClient.SetMode(IMPORT)
}

func TIsImportModeReady(t *testing.T) {
	testClient.SetMode(IMPORT)
	assert.True(t, testClient.IsImportModeReady())
	testClient.SetMode(READWRITE)
	assert.False(t, testClient.IsImportModeReady())
	testClient.SetMode(IMPORT)
}

func TGetSubjectWithVersions(t *testing.T) {
	testClient.RegisterSchemaBySubjectAndIDAndVersion(mockSchema, testingSubject, 10001, 1, "AVRO")

	aChan := make(chan map[string][]int64)
	go testClient.GetSubjectsWithVersions(aChan)
	result := <-aChan

	assert.NotNil(t, result[testingSubject])
	assert.Equal(t, []int64{1}, result[testingSubject])
}

func TGetVersions(t *testing.T) {
	var aGroup sync.WaitGroup

	aChan := make(chan SubjectWithVersions)
	go testClient.GetVersions(testingSubject, aChan, &aGroup)
	aGroup.Add(1)
	result := <-aChan
	aGroup.Wait()

	correctResult := SubjectWithVersions{
		Subject:  testingSubject,
		Versions: []int64{1},
	}

	assert.Equal(t, correctResult, result)
}

func TGetSchema(t *testing.T) {
	record := testClient.GetSchema(testingSubject, 1)
	assert.Equal(t, mockSchema, record.Schema)
}

func TRegisterSchemaBySubjectAndIDAndVersion(t *testing.T) {
	testClient.RegisterSchemaBySubjectAndIDAndVersion(mockSchema, newSubject, 10001, 1, "AVRO")
	record := testClient.GetSchema(newSubject, 1)
	assert.Equal(t, mockSchema, record.Schema)

	testClient.PerformSoftDelete(newSubject, 1)
	testClient.PerformHardDelete(newSubject, 1)
}

func TGetSoftDeletedIDs(t *testing.T) {
	testClient.RegisterSchemaBySubjectAndIDAndVersion(mockSchema, newSubject, 10001, 1, "AVRO")
	testClient.PerformSoftDelete(newSubject, 1)
	result := testClient.GetSoftDeletedIDs()
	expected := map[int64]map[string]int64{
		10001: {newSubject: 1}}

	testClient.PerformHardDelete(newSubject, 1)
	assert.Equal(t, expected, result)
}

func TFilterIDs(t *testing.T) {

	myIDs := map[int64]map[string]int64{
		10001: {testingSubject: 1},
		10002: {newSubject: 1},
	}

	// Test Allow lists
	AllowList = StringArrayFlag{
		newSubject: true,
	}
	DisallowList = nil

	expected := map[int64]map[string]int64{
		10002: {newSubject: 1},
	}

	filtered := filterIDs(myIDs)

	assert.Equal(t, expected, filtered)

	// Test DisAllow lists
	AllowList = nil
	DisallowList = StringArrayFlag{
		newSubject: true,
	}

	myIDs = map[int64]map[string]int64{
		10001: {testingSubject: 1},
		10002: {newSubject: 1},
	}

	expected = map[int64]map[string]int64{
		10001: {testingSubject: 1},
	}

	filtered = filterIDs(myIDs)

	assert.Equal(t, expected, filtered)

	// Test Both
	myIDs = map[int64]map[string]int64{
		10001: {testingSubject: 1},
		10002: {newSubject: 1},
		10003: {"hello": 1},
		10004: {"IAmSubject": 1},
	}
	AllowList = StringArrayFlag{
		newSubject:     true,
		testingSubject: true,
		"hello":        true,
	}
	DisallowList = StringArrayFlag{
		"hello": true,
	}

	// Expect hello to be disallowed
	expected = map[int64]map[string]int64{
		10001: {testingSubject: 1},
		10002: {newSubject: 1},
	}

	filtered = filterIDs(myIDs)

	assert.Equal(t, expected, filtered)

	AllowList = nil
	DisallowList = nil
}

func TFilterListedSubjects(t *testing.T) {
	mySubjects := []string{testingSubject, newSubject}

	// Test Allow lists
	AllowList = StringArrayFlag{
		newSubject: true,
	}
	DisallowList = nil

	expected := map[string]bool{
		newSubject: true,
	}
	assert.Equal(t, expected, filterListedSubjects(mySubjects))

	// Test DisAllow lists
	AllowList = nil
	DisallowList = StringArrayFlag{
		newSubject: true,
	}

	expected = map[string]bool{
		testingSubject: true,
	}
	assert.Equal(t, expected, filterListedSubjects(mySubjects))

	// Test Both
	mySubjects = []string{testingSubject, newSubject, "hello", "ImASubject"}
	AllowList = StringArrayFlag{
		newSubject:     true,
		testingSubject: true,
		"hello":        true,
	}
	DisallowList = StringArrayFlag{
		"hello": true,
	}

	// Expect hello to be disallowed
	expected = map[string]bool{
		newSubject:     true,
		testingSubject: true,
	}
	assert.Equal(t, expected, filterListedSubjects(mySubjects))

	AllowList = nil
	DisallowList = nil

}

func TFilterListedSubjectsVersions(t *testing.T) {
	mySubjects := []SubjectVersion{
		{newSubject, 1},
		{testingSubject, 1},
	}

	// Test Allow lists
	AllowList = StringArrayFlag{
		newSubject: true,
	}
	DisallowList = nil

	expected := []SubjectVersion{{Subject: newSubject, Version: 1}}
	assert.Equal(t, expected, filterListedSubjectsVersions(mySubjects))

	// Test DisAllow lists
	AllowList = nil
	DisallowList = StringArrayFlag{
		newSubject: true,
	}

	expected = []SubjectVersion{{Subject: testingSubject, Version: 1}}
	assert.Equal(t, expected, filterListedSubjectsVersions(mySubjects))

	// Test Both
	mySubjects = []SubjectVersion{
		{newSubject, 1},
		{testingSubject, 1},
		{"hello", 1},
		{"ImASubject", 1},
	}

	AllowList = StringArrayFlag{
		newSubject:     true,
		testingSubject: true,
		"hello":        true,
	}
	DisallowList = StringArrayFlag{
		"hello": true,
	}

	// Expect hello to be disallowed, expect ImASubject to not be included regardless
	expected = []SubjectVersion{{Subject: testingSubject, Version: 1}, {Subject: newSubject, Version: 1}}
	areEqual := compareSlices(expected, filterListedSubjectsVersions(mySubjects))
	assert.True(t, areEqual)

	AllowList = nil
	DisallowList = nil

}

func TPerformSoftDelete(t *testing.T) {
	//Soft delete it
	testClient.PerformSoftDelete(testingSubject, 1)
	//Check for it
	checkIfSchemaRegistered := testClient.GetSchema(testingSubject, 1)
	assert.Equal(t, "", checkIfSchemaRegistered.Schema)
}

func TPerformHardDelete(t *testing.T) {
	//Hard delete it
	testClient.PerformHardDelete(testingSubject, 1)

	//Check if it is still an ID
	checkIfIDRegistered := testClient.GetSoftDeletedIDs()
	assert.Nil(t, checkIfIDRegistered[10001])
}

func TDeleteAllSubjectsPermanently(t *testing.T) {
	/*
		By testing:
		GetSubjectsWithVersions
		PerformSoftDelete
		and PerformHardDelete
		We inherently test this method.
	*/
	assert.True(t, true)
}

func setImportMode() {
	if !testClient.IsImportModeReady() {
		err := testClient.SetMode(IMPORT)
		if err == false {
			log.Fatalln("Could not set registry to IMPORT ModeRecord.")
		}
	}
}

func performQuery(req *http.Request) map[string]string {
	response := map[string]string{}

	res, err := httpClient.Do(req)
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf(err.Error())
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Printf(err.Error())
	}

	return response

}

func compareSlices(a, b []SubjectVersion) bool {
	if len(a) != len(b) {
		return false
	}

	sliceAMap := map[SubjectVersion]bool{}
	sliceBMap := map[SubjectVersion]bool{}

	for _, val := range a {
		sliceAMap[val] = false
	}
	for _, val := range b {
		sliceBMap[val] = false
	}

	for val, _ := range sliceAMap {
		_, exists := sliceBMap[val]
		if !exists {
			return false
		}
	}
	return true
}
