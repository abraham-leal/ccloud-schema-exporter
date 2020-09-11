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
			client = SchemaRegistryClient{SRUrl: DestGetSRUrl(),SRApiKey: DestGetAPIKey(), SRApiSecret: DestGetAPISecret(), InMemSchemas: map[string][]int64{}, InMemIDs: map[int64]map[string]int64{}}
		}
		if (target == "src"){
			client = SchemaRegistryClient{SRUrl: SrcGetSRUrl(),SRApiKey: SrcGetAPIKey(), SRApiSecret: SrcGetAPISecret(), InMemSchemas: map[string][]int64{}, InMemIDs: map[int64]map[string]int64{}}
		}
	} else {
		// Enables passing in the vars through flags
		client = SchemaRegistryClient{SRUrl: SR,SRApiKey: apiKey, SRApiSecret: apiSecret, InMemSchemas: map[string][]int64{}, InMemIDs: map[int64]map[string]int64{}}
	}

	httpClient = http.Client{
		Timeout: time.Second * time.Duration(HttpCallTimeout),
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

func (src *SchemaRegistryClient) GetSubjectsWithVersions(chanY chan <- map[string][]int64) {
	src.InMemSchemas = make(map[string][]int64)
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

	// Convert back to slice for speed of iteration
	filteredSubjects := []string{}
	for key, _ := range filterListedSubjects(response) {
		filteredSubjects = append(filteredSubjects, key)
	}

	response = filteredSubjects

	// Start async fetching

	var aGroup sync.WaitGroup
	aChan := make(chan SubjectWithVersions, 10000)

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

func (src *SchemaRegistryClient) GetVersions(subject string, chanX chan <- SubjectWithVersions, wg *sync.WaitGroup) {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions", src.SRUrl,subject)
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	response :=  []int64{}

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

func (src *SchemaRegistryClient) RegisterSchemaBySubjectAndIDAndVersion (schema string, subject string, id int64, version int64, SType string) io.ReadCloser {
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
	destSubjects := make (map[string][]int64)
	destChan := make(chan map[string][]int64)

	// Account for allow/disallow lists, since this
	// method is expected to delete ALL Schemas,
	// The lists are not respected
	holderAllow := AllowList
	holderDisallow := DisallowList

	AllowList = nil
	DisallowList = nil
	go src.GetSubjectsWithVersions(destChan)
	destSubjects = <- destChan

	AllowList = holderAllow
	DisallowList = holderDisallow

	//Must perform soft delete before hard delete
	for subject, versions := range destSubjects {
		for _ , version := range versions {
			if src.PerformSoftDelete(subject, version) {
				go src.PerformHardDelete(subject, version)
			}
		}
	}
}

func (src *SchemaRegistryClient) PerformSoftDelete(subject string, version int64) bool {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%d",src.SRUrl,subject,version)
	req := GetNewRequest("DELETE", endpoint, src.SRApiKey, src.SRApiSecret,nil)
	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}

	return handleDeletes(res.Body, res.StatusCode, req.Method, endpoint, "Soft", subject, version)
}

func (src *SchemaRegistryClient) PerformHardDelete(subject string, version int64) bool {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%d?permanent=true",src.SRUrl,subject,version)
	req := GetNewRequest("DELETE", endpoint, src.SRApiKey, src.SRApiSecret,nil)
	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
	}

	return handleDeletes(res.Body, res.StatusCode, req.Method, endpoint, "Hard", subject, version)
}

func (src *SchemaRegistryClient) GetAllIDs (aChan chan <- map[int64]map[string]int64) {
	src.InMemIDs = map[int64]map[string]int64{} // Map of ID -> (Map of subject -> Version)

	var aGroup sync.WaitGroup
	//Generate bounds
	for i := LowerBound; i <= UpperBound; i++ {
		aGroup.Add(1)
		/*
			Rate limit: In order not to saturate the SR instances, we limit the rate at which we send discovery requests.
			The idea being that if one SR instance can handle the load, a cluster should be able to handle it
			even more easily. During testing, it was found that 1ms is an ideal delay for best sync performance.
		*/
		time.Sleep(time.Duration(1) * time.Millisecond)

		go src.isID(i, &aGroup)
	}
	aGroup.Wait()

	aChan <- src.InMemIDs

}

func (src *SchemaRegistryClient) isID ( id int64, wg *sync.WaitGroup) {

	httpClient := http.Client{
		Timeout: time.Second * time.Duration(10),
	}

	endpoint := fmt.Sprintf("%s/schemas/ids/%d/versions", src.SRUrl, id)
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret, nil)
	res, err := httpClient.Do(req)
	if err != nil {
		log.Println(err.Error())
		return
	}

	var manyPairs []SubjectVersion
	var tempMapOfSubjectVersion = map[string]int64{}
	body, err := ioutil.ReadAll(res.Body)
	check(err)

	if res.StatusCode == 200 {
		mutex.Lock()
		json.Unmarshal(body, &manyPairs)
		manyPairs = filterListedSubjectsVersions(manyPairs)
		for _ , subjectVer := range manyPairs {
			tempMapOfSubjectVersion[subjectVer.Subject] = subjectVer.Version
		}
		if len(tempMapOfSubjectVersion) != 0 { // Do not add IDs with empty subject-version references
			src.InMemIDs[id] = tempMapOfSubjectVersion
		}

		mutex.Unlock()
	}

	defer wg.Done()
}

func handleDeletes (body io.Reader, statusCode int, method string, endpoint string,
	reqType string, subject string, version int64) bool {
	if statusCode != 200 {
		body, _ := ioutil.ReadAll(body)
		errorMsg := fmt.Sprintf(statusError, statusCode, method, endpoint)
		log.Printf("ERROR: %s, HTTP Response: %s",errorMsg, string(body))
	} else {
		log.Println(fmt.Sprintf("%s deleted subject: %s, version: %d", reqType, subject, version))
		return true
	}
	return false
}

func handleNotSuccess (body io.Reader, statusCode int, method string, endpoint string){
	if statusCode != 200 {
		body, _ := ioutil.ReadAll(body)
		errorMsg := fmt.Sprintf(statusError, statusCode, method, endpoint)
		log.Printf("ERROR: %s, HTTP Response: %s",errorMsg, string(body))
	}
}

func filterListedSubjects (response []string) map[string]bool {
	// Start allow list work
	subjectMap := map[string]bool{}

	for _ ,s := range response { // Generate a map of subjects for easier manipulation
		subjectMap[s] = true
	}

	for s, _ := range subjectMap { // Filter out for allow lists
		if AllowList != nil { // If allow list is defined
			_, allowContains := AllowList[s]
			if !allowContains { // If allow list does not contain it, delete it
				delete(subjectMap, s)
			}
		}
		if DisallowList != nil { // If disallow list is defined
			_, disallowContains := DisallowList[s]
			if disallowContains { // If disallow list contains it, delete it
				delete(subjectMap, s)
			}
		}
	}

	return subjectMap
}


func filterListedSubjectsVersions (response []SubjectVersion) []SubjectVersion {
	subjectMap := map[string]int64{}

	for _ ,s := range response { // Generate a map of subjects for easier manipulation
		subjectMap[s.Subject] = s.Version
	}

	for s, _ := range subjectMap { // Filter out for allow lists
		if AllowList != nil { // If allow list is defined
			_, allowContains := AllowList[s]
			if !allowContains { // If allow list does not contain it, delete it
				delete(subjectMap, s)
			}
		}
		if DisallowList != nil { // If disallow list is defined
			_, disallowContains := DisallowList[s]
			if disallowContains { // If disallow list contains it, delete it
				delete(subjectMap, s)
			}
		}
	}

	subjectVersionSlice := []SubjectVersion{}
	for s, v := range subjectMap {
		tempSubjVer := SubjectVersion{
			Subject: s,
			Version: v,
		}
		subjectVersionSlice = append(subjectVersionSlice, tempSubjVer)
	}

	return subjectVersionSlice
}
