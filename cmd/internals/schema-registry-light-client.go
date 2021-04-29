package client

//
// schema-registry-light-client.go
// Copyright 2020 Abraham Leal
//

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

/**
Creates a lightweight client to schema registry in order to handle the necessary operations
for syncing schema registries. Comes with helped methods for deletions that are exported.
All functions are methods of SchemaRegistryClient
*/

var statusError = "Received status code %d instead of 200 for %s, on %s"

// Base construct of a Schema Registry Client.
// The target parameter will decide what checks will be performed in the environment variables
func NewSchemaRegistryClient(SR string, apiKey string, apiSecret string, target string) *SchemaRegistryClient {
	client := SchemaRegistryClient{}

	// If the parameters are empty, go fetch from env
	if SR == "" || apiKey == "" || apiSecret == "" {
		if target == "dst" {
			client = SchemaRegistryClient{SRUrl: DestGetSRUrl(), SRApiKey: DestGetAPIKey(), SRApiSecret: DestGetAPISecret()}
		}
		if target == "src" {
			client = SchemaRegistryClient{SRUrl: SrcGetSRUrl(), SRApiKey: SrcGetAPIKey(), SRApiSecret: SrcGetAPISecret()}
		}
	} else {
		// Enables passing in the vars through flags
		client = SchemaRegistryClient{SRUrl: SR, SRApiKey: apiKey, SRApiSecret: apiSecret}
	}

	httpClient = http.Client{
		Timeout: time.Second * time.Duration(HttpCallTimeout),
	}

	return &client
}

// Returns whether a proper connection could be made to the Schema Registry by the client
func (src *SchemaRegistryClient) IsReachable() bool {
	endpoint := src.SRUrl
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)

	res, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return true
	} else {
		return false
	}
}

// Returns all non-deleted (soft or hard deletions) subjects with their versions in the form of a map.
func (src *SchemaRegistryClient) GetSubjectsWithVersions(chanY chan<- map[string][]int64, deleted bool) {
	endpoint := ""
	if deleted {
		endpoint = fmt.Sprintf("%s/subjects?deleted=true", src.SRUrl)
	} else {
		endpoint = fmt.Sprintf("%s/subjects", src.SRUrl)
	}

	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)

	res, err := httpClient.Do(req)
	check(err)
	defer res.Body.Close()
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	response := []string{}

	body, err := ioutil.ReadAll(res.Body)
	checkDontFail(err)

	err = json.Unmarshal(body, &response)
	checkDontFail(err)

	// Convert back to slice for speed of iteration
	filteredSubjects := []string{}
	for key, _ := range filterListedSubjects(response) {
		filteredSubjects = append(filteredSubjects, key)
	}

	response = filteredSubjects

	// Start async fetching
	var aGroup sync.WaitGroup
	aChan := make(chan SubjectWithVersions)

	for _, s := range response {
		aGroup.Add(1)
		/*
			Rate limit: In order not to saturate the SR instances, we limit the rate at which we send schema discovery requests.
			The idea being that if one SR instance can handle the load, a cluster should be able to handle it
			even more easily. During testing, it was found that 2ms is an ideal delay for best sync performance.
		*/
		time.Sleep(time.Duration(2) * time.Millisecond)
		go src.GetVersions(s, aChan, &aGroup, deleted)
	}
	go func() {
		aGroup.Wait()
		close(aChan)
	}()

	//Collect SubjectWithVersions
	tmpSchemaMap := make(map[string][]int64)
	for item := range aChan {
		tmpSchemaMap[item.Subject] = item.Versions
	}

	// Send back to main thread
	chanY <- tmpSchemaMap
}

// Returns all non-deleted versions (soft or hard deleted) that exist for a given subject.
func (src *SchemaRegistryClient) GetVersions(subject string, chanX chan<- SubjectWithVersions, wg *sync.WaitGroup, deleted bool) {
	endpoint := ""
	if deleted {
		endpoint = fmt.Sprintf("%s/subjects/%s/versions?deleted=true", src.SRUrl, url.QueryEscape(subject))
	} else {
		endpoint = fmt.Sprintf("%s/subjects/%s/versions", src.SRUrl, url.QueryEscape(subject))
	}

	defer wg.Done()
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
		return
	}
	defer res.Body.Close()
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	response := []int64{}

	body, err := ioutil.ReadAll(res.Body)
	checkDontFail(err)

	err = json.Unmarshal(body, &response)
	checkDontFail(err)

	pkgSbj := SubjectWithVersions{
		Subject:  subject,
		Versions: response,
	}

	// Send back to retrieving thread
	chanX <- pkgSbj
}

// Performs a check to see if the compatibility level for the backing Schema Registry is set to NONE
func (src *SchemaRegistryClient) IsCompatReady() bool {
	response, ok := handleEndpointQuery("config", src)
	if !ok {
		return false
	}

	if response["compatibilityLevel"] == NONE.String() {
		return true
	} else {
		return false
	}
}

// Allows to set a compatibility level for the backing Schema Registry, returns true if successful
func (src *SchemaRegistryClient) SetGlobalCompatibility(comptToSet Compatibility) bool {
	endpoint := fmt.Sprintf("%s/config", src.SRUrl)

	compat := CompatRecord{Compatibility: comptToSet.String()}
	toSend, err := json.Marshal(compat)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	req := GetNewRequest("PUT", endpoint, src.SRApiKey, src.SRApiSecret, nil, bytes.NewReader(toSend))

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
		return false
	}
	defer res.Body.Close()

	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	if res.StatusCode == 200 {
		return true
	} else {
		return false
	}

}

// Performs a check on the backing Schema Registry to see if it is in global IMPORT mode.
func (src *SchemaRegistryClient) IsImportModeReady() bool {
	response, ok := handleEndpointQuery("mode", src)
	if !ok {
		return false
	}

	if response["mode"] == IMPORT.String() {
		return true
	} else {
		return false
	}
}

// Allows to set a global mode for the backing Schema Registry
func (src *SchemaRegistryClient) SetMode(modeToSet Mode) bool {
	endpoint := fmt.Sprintf("%s/mode", src.SRUrl)

	mode := ModeRecord{Mode: modeToSet.String()}
	modeToSend, err := json.Marshal(mode)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	req := GetNewRequest("PUT", endpoint, src.SRApiKey, src.SRApiSecret, nil, bytes.NewReader(modeToSend))

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
		return false
	}
	defer res.Body.Close()

	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	if res.StatusCode == 200 {
		return true
	} else {
		return false
	}

}

// Returns a SchemaRecord for the given subject and version by querying the backing Schema Registry
func (src *SchemaRegistryClient) GetSchema(subject string, version int64, deleted bool) SchemaRecord {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%d", src.SRUrl, url.QueryEscape(subject), version)
	if deleted {
		endpoint = fmt.Sprintf("%s/subjects/%s/versions/%d?deleted=true", src.SRUrl, url.QueryEscape(subject), version)
	}
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
		log.Println("Could not retrieve schema!")
		return SchemaRecord{}
	}
	defer res.Body.Close()

	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	schemaResponse := new(SchemaRecord)

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf(err.Error())
		return SchemaRecord{}
	}

	err = json.Unmarshal(body, &schemaResponse)
	checkDontFail(err)

	return SchemaRecord{Subject: schemaResponse.Subject, Schema: schemaResponse.Schema, Version: schemaResponse.Version, Id: schemaResponse.Id, SType: schemaResponse.SType, References: schemaResponse.References}.setTypeIfEmpty().setReferenceIfEmpty()
}

// Registers a schema
func (src *SchemaRegistryClient) RegisterSchema(schema string, subject string, SType string, references []SchemaReference) []byte {
	return src.RegisterSchemaBySubjectAndIDAndVersion(schema, subject, 0, 0, SType, references)
}

// Registers a schema with the given SchemaID and SchemaVersion
func (src *SchemaRegistryClient) RegisterSchemaBySubjectAndIDAndVersion(schema string, subject string, id int64, version int64, SType string, references []SchemaReference) []byte {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions", src.SRUrl, url.QueryEscape(subject))

	schemaRequest := SchemaToRegister{}
	if id == 0 && version == 0 {
		schemaRequest = SchemaToRegister{Schema: schema, SType: SType, References: references}
	} else {
		schemaRequest = SchemaToRegister{Schema: schema, Id: id, Version: version, SType: SType, References: references}
	}
	schemaJSON, err := json.Marshal(schemaRequest)
	if err != nil {
		log.Printf(err.Error())
	}

	req := GetNewRequest("POST", endpoint, src.SRApiKey, src.SRApiSecret, nil, bytes.NewReader(schemaJSON))

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
		log.Println("Could not register schema!")
		return nil
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	checkDontFail(err)

	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	if WithMetrics && res.StatusCode == 200 {
		schemasRegistered.Inc()
	}

	return body
}

// Deletes all schemas in the backing Schema Registry, including previously soft deleted subjects
// This method does not respect AllowList or DisallowList
func (src *SchemaRegistryClient) DeleteAllSubjectsPermanently() {
	destSubjects := make(map[string][]int64)
	destChan := make(chan map[string][]int64)

	// Account for allow/disallow lists, since this
	// method is expected to delete ALL Schemas,
	// The lists are not respected
	holderAllow := AllowList
	holderDisallow := DisallowList

	AllowList = nil
	DisallowList = nil
	go src.GetSubjectsWithVersions(destChan, true)
	destSubjects = <-destChan

	AllowList = holderAllow
	DisallowList = holderDisallow

	//Must perform soft delete before hard delete
	for subject, versions := range destSubjects {
		for _, version := range versions {
			if src.subjectExists(subject) {
				if src.PerformSoftDelete(subject, version) {
					src.PerformHardDelete(subject, version)
				}
			}
		}
	}
}

// Performs a Soft Delete on the given subject and version on the backing Schema Registry
func (src *SchemaRegistryClient) PerformSoftDelete(subject string, version int64) bool {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%d", src.SRUrl, url.QueryEscape(subject), version)
	req := GetNewRequest("DELETE", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)
	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	// We've confirmed this subject does not exist
	if res.StatusCode == 404 {
		return true
	}

	// Handle referenced subjects
	if res.StatusCode == 422 {

		regBody, err := ioutil.ReadAll(res.Body)
		check(err)

		errMsg := ErrorMessage{}

		err = json.Unmarshal(regBody, &errMsg)
		check(err)

		if errMsg.ErrorCode == 42206 {
			referencesEndpoint := fmt.Sprintf("%s/subjects/%s/versions/%d/referencedby", src.SRUrl, url.QueryEscape(subject), version)
			findReferencingSchemas := GetNewRequest("GET", referencesEndpoint, src.SRApiKey, src.SRApiSecret, nil, nil)

			refRes, err := httpClient.Do(findReferencingSchemas)
			checkDontFail(err)
			defer refRes.Body.Close()
			refBody, err := ioutil.ReadAll(refRes.Body)
			checkDontFail(err)

			idsReferencing := []int64{}

			err = json.Unmarshal(refBody, &idsReferencing)
			checkDontFail(err)

			for _, thisId := range idsReferencing {
				correlatedSubjectVersionsEndpoint := fmt.Sprintf("%s/schemas/ids/%d/versions", src.SRUrl, thisId)

				findReferencingSubjectVersions := GetNewRequest("GET", correlatedSubjectVersionsEndpoint, src.SRApiKey, src.SRApiSecret, nil, nil)

				refSVRes, err := httpClient.Do(findReferencingSubjectVersions)
				checkDontFail(err)
				defer refSVRes.Body.Close()
				refSVBody, err := ioutil.ReadAll(refSVRes.Body)
				checkDontFail(err)

				schemaVersionsReferencing := []SubjectVersion{}

				err = json.Unmarshal(refSVBody, &schemaVersionsReferencing)
				checkDontFail(err)

				for _, subjectVersion := range schemaVersionsReferencing {
					if src.PerformSoftDelete(subjectVersion.Subject, subjectVersion.Version) {
						src.PerformHardDelete(subjectVersion.Subject, subjectVersion.Version)
					}
				}
			}

			// Attempt to delete original schema now that it isn't being referenced anymore.
			return src.PerformSoftDelete(subject, version)

		} else {
			return false
		}
	}

	return handleDeletesHTTPResponse(res.Body, res.StatusCode, req.Method, endpoint, "Soft", subject, version)
}

// Performs a Hard Delete on the given subject and version on the backing Schema Registry
// NOTE: A Hard Delete should only be performed after a soft delete
func (src *SchemaRegistryClient) PerformHardDelete(subject string, version int64) bool {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions/%d?permanent=true", src.SRUrl, url.QueryEscape(subject), version)
	req := GetNewRequest("DELETE", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)
	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	return handleDeletesHTTPResponse(res.Body, res.StatusCode, req.Method, endpoint, "Hard", subject, version)
}

// Returns a map with the [ID][Subject:Versions] state of the backing Schema Registry for only the soft deleted SubjectVersions
func (src *SchemaRegistryClient) GetSoftDeletedIDs() map[int64]map[string][]int64 {

	responseWithDeletes := src.getSchemaList(true)
	responseWithOutDeletes := src.getSchemaList(false)

	diff := GetIDDiff(responseWithDeletes, responseWithOutDeletes)
	return filterIDs(diff)
}

// Returns a dump of all Schemas mapped to their IDs from the backing Schema Registry
// The parameter specifies whether to show soft deleted schemas as well
func (src *SchemaRegistryClient) getSchemaList(deleted bool) map[int64]map[string][]int64 {
	endpoint := fmt.Sprintf("%s/schemas?deleted=%v", src.SRUrl, deleted)
	if !deleted {
		endpoint = fmt.Sprintf("%s/schemas", src.SRUrl)
	}

	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)
	res, err := httpClient.Do(req)
	check(err)
	defer res.Body.Close()

	response := []SchemaExtraction{}

	body, err := ioutil.ReadAll(res.Body)
	checkDontFail(err)

	err = json.Unmarshal(body, &response)
	checkDontFail(err)

	responseMap := make(map[int64]map[string][]int64)

	for _, schema := range response {
		currentStateOfID, haveSeenIDBefore := responseMap[schema.Id]
		if haveSeenIDBefore {
			_, haveSeenSubject := currentStateOfID[schema.Subject]
			if haveSeenSubject {
				responseMap[schema.Id][schema.Subject] = append(responseMap[schema.Id][schema.Subject], schema.Version)
			} else {
				tempMap := responseMap[schema.Id]
				tempMap[schema.Subject] = []int64{schema.Version}
				responseMap[schema.Id] = tempMap
			}
		} else {
			responseMap[schema.Id] = map[string][]int64{schema.Subject: {schema.Version}}
		}

	}

	return responseMap
}

// Checks if the subject is in the backing schema registry, regardless of it is soft deleted or not.
func (src *SchemaRegistryClient) subjectExists(subject string) bool {
	endpoint := fmt.Sprintf("%s/subjects/%s/versions?deleted=true", src.SRUrl, url.QueryEscape(subject))
	sbjReq := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)
	response, err := httpClient.Do(sbjReq)
	check(err)
	defer response.Body.Close()

	if response.StatusCode == 200 {
		return true
	} else {
		return false
	}
}

func (src *SchemaRegistryClient) schemaIsRegisteredUnderSubject(subject string, schemaType string, schema string, references []SchemaReference) bool {
	endpoint := fmt.Sprintf("%s/subjects/%s", src.SRUrl, url.QueryEscape(subject))

	schemaRequest := SchemaToRegister{Schema: schema, SType: schemaType, References: references}

	schemaJSON, err := json.Marshal(schemaRequest)
	if err != nil {
		log.Printf(err.Error())
	}

	sbjReq := GetNewRequest("POST", endpoint, src.SRApiKey, src.SRApiSecret, nil, bytes.NewReader(schemaJSON))
	response, err := httpClient.Do(sbjReq)
	defer response.Body.Close()

	if response.StatusCode > 400 {
		return false
	}

	return true
}
