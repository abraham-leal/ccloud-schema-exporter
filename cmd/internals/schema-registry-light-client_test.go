package client

//
// schema-registry-light-client-test.go
// Author: Abraham Leal
//

import (
	"encoding/json"
	"github.com/dankinder/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"html"
	"io/ioutil"
	"testing"
)

func TestGetSubjectWithVersion(t *testing.T) {
	registryHandler := &httpmock.MockHandler{}

	registryHandler.On("Handle","GET", "/subjects", mock.Anything).Return(
		httpmock.Response{ Body: []byte(`["testSubject"]`),
		})
	registryHandler.On("Handle","GET", "/subjects/testSubject/versions", mock.Anything).Return(
		httpmock.Response{ Body: []byte(`[1]`),
		})

	server := httpmock.NewServer(registryHandler)
	defer server.Close()

	testSClient := NewSchemaRegistryClient(server.URL(), "mockKey", "mockSecret","src")

	aChan := make(chan map[string][]int64)
	go testSClient.GetSubjectsWithVersions(aChan)
	result := <- aChan

	assert.NotNil(t, result["testSubject"])
	assert.Equal(t, []int64{1}, result["testSubject"])

	registryHandler.AssertExpectations(t)
}


func TestModeReadiness(t *testing.T) {
	registryHandler := &httpmock.MockHandler{}
	memMode := "IMPORT"
	mockModeRecord := &ModeRecord{Mode: memMode}
	modeJson, _ := json.Marshal(mockModeRecord)

	registryHandler.On("Handle","PUT", "/mode", httpmock.JSONMatcher(mockModeRecord)).Return(
		httpmock.Response{ Body: modeJson,
		})

	registryHandler.On("Handle","GET", "/mode", mock.Anything).Return(
		httpmock.Response{ Body: modeJson,
		})

	server := httpmock.NewServer(registryHandler)
	defer server.Close()

	testSClient := NewSchemaRegistryClient(server.URL(), "mockKey", "mockSecret","src")

	assert.True(t, testSClient.IsImportModeReady())
	assert.True(t, testSClient.SetMode(memMode))

	registryHandler.AssertExpectations(t)
}



func TestGetSchema(t *testing.T) {
	registryHandler := &httpmock.MockHandler{}
	mockSchema := SchemaRecord{
		Subject: "test",
		Schema:  "{\"field\" : \"value\"}",
		SType:   "AVRO",
		Version: 1,
		Id:      10001,
	}
	schemaJson, _ := json.Marshal(mockSchema)
	registryHandler.On("Handle","GET", "/subjects/testSubject/versions/1", mock.Anything).Return(
		httpmock.Response{ Body: schemaJson,
		})

	server := httpmock.NewServer(registryHandler)
	defer server.Close()

	testSClient := NewSchemaRegistryClient(server.URL(), "mockKey", "mockSecret","src")

	record := testSClient.GetSchema("testSubject", 1)

	assert.Equal(t, mockSchema,record)

	registryHandler.AssertExpectations(t)
}


func TestSchemaRegistration(t *testing.T) {
	registryHandler := &httpmock.MockHandler{}
	mockSchema := SchemaRecord{
		Subject: "test",
		Schema:  "{\"field\" : \"value\"}",
		SType:   "AVRO",
		Version: 1,
		Id:      10001,
	}
	schemaJson, _ := json.Marshal(mockSchema)
	registryHandler.On("Handle","POST", "/subjects/testSubject/versions", mock.Anything).Return(
		httpmock.Response{ Body: schemaJson,
		})

	server := httpmock.NewServer(registryHandler)
	defer server.Close()

	testSClient := NewSchemaRegistryClient(server.URL(), "mockKey", "mockSecret","src")

	body, _ := ioutil.ReadAll(testSClient.RegisterSchemaBySubjectAndIDAndVersion(
		html.EscapeString(string(schemaJson)),
		"testSubject",
		1,
		10001,
		"AVRO"))

	gotStruct := SchemaRecord{}
	json.Unmarshal(body,&gotStruct)

	assert.Equal(t, gotStruct.Schema, mockSchema.Schema)

	registryHandler.AssertExpectations(t)
}
