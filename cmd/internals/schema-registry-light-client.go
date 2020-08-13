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
	"net/http"
	"strconv"
	"sync"
	"time"
)

func NewSchemaRegistryClient(SR string, API_KEY string, API_SECRET string, target string) SchemaRegistryClient {
	client := SchemaRegistryClient{}

	// If the paramethers are empty, go fetch from env
	if (SR == "" || API_KEY == "" || API_SECRET == "") {
		if (target == "dst") {
			client = SchemaRegistryClient{SRUrl: DestGetSRUrl(),SRApiKey: DestGetAPIKey(), SRApiSecret: DestGetAPISecret(), InMemSchemas: map[string][]int{}}
		}
		if (target == "src"){
			client = SchemaRegistryClient{SRUrl: SrcGetSRUrl(),SRApiKey: SrcGetAPIKey(), SRApiSecret: SrcGetAPISecret(), InMemSchemas: map[string][]int{}}
		}
	} else {
		// Enables passing in the vars through flags
		client = SchemaRegistryClient{SRUrl: SR,SRApiKey: API_KEY, SRApiSecret: API_SECRET, InMemSchemas: map[string][]int{}}
	}

	httpClient = http.Client{
		Timeout: time.Second * time.Duration(httpCallTimeout),
	}

	return client
}

func (src SchemaRegistryClient) IsReachable() bool {
	endpoint := src.SRUrl
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if res.StatusCode == 200 {return true} else {return false}

}

func (src SchemaRegistryClient) GetSubjectsWithVersions(chanY chan <- map[string][]int) {
	endpoint := src.SRUrl+"/subjects"
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if res.StatusCode != 200 {
		body, _ := ioutil.ReadAll(res.Body)
		errorMsg := "Received status code " + strconv.FormatInt(int64(res.StatusCode),10) + " instead of 200 for "+ req.Method +" on " + endpoint
		fmt.Printf(errorMsg,body)
	}

	response :=  []string{}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Printf(err.Error())
	}

	err = json.Unmarshal(body,&response)
	if err != nil {
		fmt.Printf(err.Error())
	}

	var aGroup sync.WaitGroup
	aGroup.Add(len(response))
	aChan := make(chan SubjectWithVersions, 1000)

	for _, s := range response {
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

func (src SchemaRegistryClient) GetVersions (subject string, chanX chan <- SubjectWithVersions, wg *sync.WaitGroup) {
	endpoint := src.SRUrl+"/subjects/"+subject+"/versions"
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if res.StatusCode != 200 {
		body, _ := ioutil.ReadAll(res.Body)
		errorMsg := "Received status code " + strconv.FormatInt(int64(res.StatusCode),10) + " instead of 200 for "+ req.Method +" on " + endpoint
		fmt.Printf(errorMsg,body)
	}

	response :=  []int{}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Printf(err.Error())
	}

	err = json.Unmarshal(body,&response)
	if err != nil {
		fmt.Printf(err.Error())
	}

	pkgSbj := SubjectWithVersions{
		Subject:  subject,
		Versions: response,
	}

	// Send back to retrieving thread
	chanX <- pkgSbj
	wg.Done()

}

func (src SchemaRegistryClient) IsImportModeReady () bool {
	endpoint := src.SRUrl+"/mode"
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if res.StatusCode != 200 {
		body, _ := ioutil.ReadAll(res.Body)
		errorMsg := "Received status code " + strconv.FormatInt(int64(res.StatusCode),10) + " instead of 200 for "+ req.Method +" on " + endpoint
		fmt.Printf(errorMsg,body)
	}

	response := map[string]string{}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Printf(err.Error())
		return false
	}

	err = json.Unmarshal(body,&response)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if response["mode"] == "IMPORT" {
		return true
	} else {
		return false
	}
}

func (src SchemaRegistryClient) SetMode(modeToSet string) bool{
	endpoint := src.SRUrl+"/mode"

	mode := ModeRecord{Mode: modeToSet}
	modeToSend, err := json.Marshal(mode)
	if err != nil {
		fmt.Printf(err.Error())
	}

	req := GetNewRequest("PUT", endpoint, src.SRApiKey, src.SRApiSecret,bytes.NewReader(modeToSend))

	res, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if res.StatusCode != 200 {
		body, _ := ioutil.ReadAll(res.Body)
		errorMsg := "Received status code " + strconv.FormatInt(int64(res.StatusCode),10) + " instead of 200 for "+ req.Method +" on " + endpoint
		fmt.Printf(errorMsg,body)
	}

	if (res.StatusCode == 200) {
		return true
	} else {return false}

}

func (src SchemaRegistryClient) GetSchema (subject string, version int64) SchemaRecord {

	endpoint := src.SRUrl+"/subjects/"+subject+"/versions/"+strconv.FormatInt(version,10)
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret,nil)

	res, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if res.StatusCode != 200 {
		body, _ := ioutil.ReadAll(res.Body)
		errorMsg := "Received status code " + strconv.FormatInt(int64(res.StatusCode),10) + " instead of 200 for "+ req.Method +" on " + endpoint
		fmt.Printf(errorMsg,body)
	}

	schemaResponse := new(SchemaRecord)

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Printf(err.Error())
		return SchemaRecord{}
	}

	err = json.Unmarshal(body,&schemaResponse)
	if err != nil {
		fmt.Printf(err.Error())
	}

	return SchemaRecord{Subject: schemaResponse.Subject,Schema: schemaResponse.Schema, Version: schemaResponse.Version, Id: schemaResponse.Id, SType: schemaResponse.SType}.setTypeIfEmpty()
}

func (src SchemaRegistryClient) RegisterSchemaBySubjectAndIDAndVersion (schema string, subject string, id int, version int, SType string) io.ReadCloser {
	endpoint := src.SRUrl+"/subjects/"+subject+"/versions"

	schemaRequest := SchemaToRegister{Schema: schema, Id: id, Version: version, SType: SType}
	schemaJSON, err := json.Marshal(schemaRequest)
	if err != nil {
		fmt.Printf(err.Error())
	}

	req := GetNewRequest("POST", endpoint, src.SRApiKey, src.SRApiSecret,bytes.NewReader(schemaJSON))

	res, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if res.StatusCode != 200 {
		body, _ := ioutil.ReadAll(res.Body)
		errorMsg := "Received status code " + strconv.FormatInt(int64(res.StatusCode),10) + " instead of 200 for "+ req.Method +" on " + endpoint
		fmt.Printf(errorMsg,body)
	}


	return res.Body
}

// Deletes all schemas in the registry
func (src SchemaRegistryClient) DeleteAllSubjectsPermanently (){
	//Must perform soft delete before hard delete
	for subject, versions := range src.InMemSchemas {
		for _ , version := range versions {
			fmt.Println("Soft Deleting subject: " + subject + " version: " + strconv.FormatInt(int64(version),10))
			endpoint := src.SRUrl+"/subjects/"+subject+"/versions/"+strconv.FormatInt(int64(version),10)
			req := GetNewRequest("DELETE", endpoint, src.SRApiKey, src.SRApiSecret,nil)
			res, err := httpClient.Do(req)
			if err != nil {
				fmt.Printf(err.Error())
			}

			if res.StatusCode != 200 {
				body, _ := ioutil.ReadAll(res.Body)
				errorMsg := "Received status code " + strconv.FormatInt(int64(res.StatusCode),10) + " instead of 200 for "+ req.Method +" on " + endpoint
				fmt.Printf(errorMsg,body)
			} else {
				// Hard delete Async
				go src.performHardDelete(subject,version)
			}
		}
	}
}

func (src SchemaRegistryClient) performHardDelete (subject string, version int){
	fmt.Println("Permanently deleting subject: " + subject + " version: " + strconv.FormatInt(int64(version),10))
	endpoint := src.SRUrl+"/subjects/"+subject+"/versions/"+strconv.FormatInt(int64(version),10)+"?permanent=true"
	req := GetNewRequest("DELETE", endpoint, src.SRApiKey, src.SRApiSecret,nil)
	res, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(err.Error())
	}

	if res.StatusCode != 200 {
		body, _ := ioutil.ReadAll(res.Body)
		errorMsg := "Received status code " + strconv.FormatInt(int64(res.StatusCode),10) + " instead of 200 for "+ req.Method +" on " + endpoint
		fmt.Printf(errorMsg,body)
	}
}
