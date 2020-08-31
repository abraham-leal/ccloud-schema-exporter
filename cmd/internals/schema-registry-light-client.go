package client

//
// schema-registry-light-client.go
// Author: Abraham Leal
//

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)
/**
Creates a lightweight client to schema registry in order to handle the necessary operations
for syncing schema registries. Comes with helped methods for deletions that are exported.
All functions are methods of SchemaRegistryClient
 */

var statusError = "Received status code %d instead of 200 for %s, on %s"
func NewSchemaRegistryClient(SR string, apiKey string, apiSecret string, target string) *SchemaRegistryClient {
	client := SchemaRegistryClient{}

	// If the parameters are empty, go fetch from env
	if (SR == "" || apiKey == "" || apiSecret == "") {
		if (target == "dst") {
			client = SchemaRegistryClient{SRUrl: DestGetSRUrl(),SRApiKey: DestGetAPIKey(), SRApiSecret: DestGetAPISecret(), InMemSchemas: map[string][]int{}}
		}
		if (target == "src"){
			client = SchemaRegistryClient{SRUrl: SrcGetSRUrl(),SRApiKey: SrcGetAPIKey(), SRApiSecret: SrcGetAPISecret(), InMemSchemas: map[string][]int{}}
		}
	} else {
		// Enables passing in the vars through flags
		client = SchemaRegistryClient{SRUrl: SR,SRApiKey: apiKey, SRApiSecret: apiSecret, InMemSchemas: map[string][]int{}}
	}

	httpClient = http.Client{
		Timeout: time.Second * time.Duration(httpCallTimeout),
	}

	return &client
}

func (src *SchemaRegistryClient) IsReachable() bool {
	endpoint := src.SRUrl
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Fatalln(err.Error())
	}

	if res.StatusCode == 200 {return true} else {return false}

}

func (src *SchemaRegistryClient) GetSubjectsWithVersions(chanY chan <- map[string][]int) {
	src.InMemSchemas = make(map[string][]int)
	endpoint := fmt.Sprintf("%s/subjects",src.SRUrl)
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Fatalln(err.Error())
	}
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	response :=  []string{}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf(err.Error())
	}

	err = json.Unmarshal(body,&response)
	if err != nil {
		log.Printf(err.Error())
	}

	var aGroup sync.WaitGroup
	aChan := make(chan SubjectWithVersions, 1000)

	for _, s := range response {
		aGroup.Add(1)
		go src.GetVersions(s, aChan, &aGroup)
	}

	aGroup.Wait()
	close(aChan)

	//Collect SubjectWithVersions
	for item := range aChan {
		src.InMemSchemas[item.Subject] = item.Versions
	}

	// Send back to main thread
	chanY <- src.InMemSchemas
}

func (src *SchemaRegistryClient) GetVersions (subject string, chanX chan <- SubjectWithVersions, wg *sync.WaitGroup) {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions", src.SRUrl,subject)
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	response :=  []int{}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf(err.Error())
	}

	err = json.Unmarshal(body,&response)
	if err != nil {
		log.Printf(err.Error())
	}

	pkgSbj := SubjectWithVersions{
		Subject:  subject,
		Versions: response,
	}

	// Send back to retrieving thread
	chanX <- pkgSbj
	wg.Done()

}

func (src *SchemaRegistryClient) IsImportModeReady () bool {
	endpoint := fmt.Sprintf("%s/mode",src.SRUrl)
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	response := map[string]string{}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	err = json.Unmarshal(body,&response)
	if err != nil {
		log.Printf(err.Error())
	}

	if response["mode"] == "IMPORT" {
		return true
	} else {
		return false
	}
}

func (src *SchemaRegistryClient) SetMode(modeToSet string) bool{
	endpoint := fmt.Sprintf("%s/mode",src.SRUrl)

	mode := ModeRecord{Mode: modeToSet}
	modeToSend, err := json.Marshal(mode)
	if err != nil {
		log.Printf(err.Error())
	}

	req := GetNewRequest("PUT", endpoint, src.SRApiKey, src.SRApiSecret,bytes.NewReader(modeToSend))

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	if (res.StatusCode == 200) {
		return true
	} else {return false}

}

func (src *SchemaRegistryClient) GetSchema (subject string, version int64) SchemaRecord {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%d",src.SRUrl,subject,version)
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	schemaResponse := new(SchemaRecord)

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf(err.Error())
		return SchemaRecord{}
	}

	err = json.Unmarshal(body,&schemaResponse)
	if err != nil {
		log.Printf(err.Error())
	}

	return SchemaRecord{Subject: schemaResponse.Subject,Schema: schemaResponse.Schema, Version: schemaResponse.Version, Id: schemaResponse.Id, SType: schemaResponse.SType}.setTypeIfEmpty()
}

func (src *SchemaRegistryClient) RegisterSchemaBySubjectAndIDAndVersion (schema string, subject string, id int, version int, SType string) io.ReadCloser {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions",src.SRUrl,subject)

	schemaRequest := SchemaToRegister{Schema: schema, Id: id, Version: version, SType: SType}
	schemaJSON, err := json.Marshal(schemaRequest)
	if err != nil {
		log.Printf(err.Error())
	}

	req := GetNewRequest("POST", endpoint, src.SRApiKey, src.SRApiSecret,bytes.NewReader(schemaJSON))

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)


	return res.Body
}

// Deletes all schemas in the registry
func (src *SchemaRegistryClient) DeleteAllSubjectsPermanently (){
	destSubjects := make (map[string][]int)
	destChan := make(chan map[string][]int)
	go src.GetSubjectsWithVersions(destChan)
	destSubjects = <- destChan

	//Must perform soft delete before hard delete
	for subject, versions := range destSubjects {
		for _ , version := range versions {
			if src.PerformSoftDelete(subject, version) {
				go src.PerformHardDelete(subject, version)
			}
		}
	}
}

func (src *SchemaRegistryClient) PerformSoftDelete(subject string, version int) bool {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%d",src.SRUrl,subject,version)
	req := GetNewRequest("DELETE", endpoint, src.SRApiKey, src.SRApiSecret,nil)
	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}

	return handleDeletes(res.Body, res.StatusCode, req.Method, endpoint, "Soft", subject, version)
}

func (src *SchemaRegistryClient) PerformHardDelete(subject string, version int) bool {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%d?permanent=true",src.SRUrl,subject,version)
	req := GetNewRequest("DELETE", endpoint, src.SRApiKey, src.SRApiSecret,nil)
	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}

	return handleDeletes(res.Body, res.StatusCode, req.Method, endpoint, "Hard", subject, version)
}

func handleDeletes (body io.Reader, statusCode int, method string, endpoint string,
	reqType string, subject string, version int) bool {
	if statusCode != 200 {
		body, _ := ioutil.ReadAll(body)
		errorMsg := fmt.Sprintf(statusError, statusCode, method, endpoint)
		log.Printf("ERROR: %s, HTTP Response: %s",errorMsg, string(body))
	} else {
		log.Println(fmt.Sprintf("%s deleted subject: %s, version: %d", reqType, subject, version))
		return false
	}
	return true
}

func handleNotSuccess (body io.Reader, statusCode int, method string, endpoint string){
	if statusCode != 200 {
		body, _ := ioutil.ReadAll(body)
		errorMsg := fmt.Sprintf(statusError, statusCode, method, endpoint)
		log.Printf("ERROR: %s, HTTP Response: %s",errorMsg, string(body))
	}
}
