package client

//
// helpers.go
// Copyright 2020 Abraham Leal
//

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

// Simple check function that will panic if there is an error present
func check(e error) {
	if e != nil {
		panic(e)
	}
}

// Simple check function that will not fail and log if there is an error present
func checkDontFail(e error) {
	if e != nil {
		log.Println(e)
	}
}

// Prints the version of the ccloud-schema-exporter
func printVersion() {
	fmt.Printf("ccloud-schema-exporter: %s\n", Version)
}

// Checks if the described path is a file
// It will return false if the file does not exist or the given is a directory
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func isInSlice(i int64, list []int64) bool {
	for _, current := range list {
		if current == i {
			return true
		}
	}
	return false
}

func referenceIsInSlice(i SchemaReference, list []SchemaReference) bool {
	for _, current := range list {
		if current == i {
			return true
		}
	}
	return false
}

// Returns an HTTP request with the given information to execute
func GetNewRequest(method string, endpoint string, key string, secret string, headers map[string]string, reader io.Reader) *http.Request {
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		panic(err)
	}

	req.SetBasicAuth(key, secret)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "ccloud-schema-exporter/"+Version)
	req.Header.Add("Correlation-Context", "service.name=ccloud-schema-exporter,service.version="+Version)

	if headers != nil {
		for key, val := range headers {
			req.Header.Add(key, val)
		}
	}

	return req
}

// Checks if the given subject is allowed by the defined Allow and Disallow Lists
// Returns true if the subject is allowed
func checkSubjectIsAllowed(subject string) bool {
	if len(AllowList) != 0 {
		_, isAllowed := AllowList[subject]
		if !isAllowed {
			return false
		}
	}
	if len(DisallowList) != 0 {
		_, isDisallowed := DisallowList[subject]
		if isDisallowed {
			return false
		}
	}
	if len(AllowList) != 0 && len(DisallowList) != 00 {
		_, isAllowed := AllowList[subject]
		_, isDisallowed := DisallowList[subject]
		if !isAllowed || isDisallowed {
			return false
		}
	}
	return true
}

// Deletes all registered schemas from the destination SR
func deleteAllFromDestination(sr string, key string, secret string) {
	destClient := NewSchemaRegistryClient(sr, key, secret, "dst")
	if !destClient.IsReachable() {
		log.Fatalln("Could not reach source registry. Possible bad credentials?")
	}
	destClient.DeleteAllSubjectsPermanently()
}

// Handle the SR response to a delete command for a subject/version
func handleDeletesHTTPResponse(body io.ReadCloser, statusCode int, method string, endpoint string,
	reqType string, subject string, version int64) bool {
	defer body.Close()
	if statusCode != 200 {
		body, _ := ioutil.ReadAll(body)
		errorMsg := fmt.Sprintf(statusError, statusCode, method, endpoint)
		log.Printf("ERROR: %s, HTTP Response: %s", errorMsg, string(body))
	} else {
		log.Println(fmt.Sprintf("%s deleted subject: %s, version: %d", reqType, subject, version))
		if WithMetrics {
			if reqType == "Soft" {
				schemasSoftDeleted.Inc()
			}
			if reqType == "Hard" {
				schemasHardDeleted.Inc()
			}
		}
		return true
	}
	return false
}

// Handles the case where an HTTP call was not successful generically
func handleNotSuccess(body io.Reader, statusCode int, method string, endpoint string) {
	if statusCode != 200 {
		body, _ := ioutil.ReadAll(body)
		errorMsg := fmt.Sprintf(statusError, statusCode, method, endpoint)
		log.Printf("ERROR: %s, HTTP Response: %s", errorMsg, string(body))
	}
}

// Filters the provided slice of subjects according to what is provided in AllowList and DisallowList
func filterListedSubjects(response []string) map[string]bool {
	// Start allow list work
	subjectMap := map[string]bool{}

	for _, s := range response { // Generate a map of subjects for easier manipulation
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

// Filters the provided slice of SubjectVersion according to what is provided in AllowList and DisallowList
func filterListedSubjectsVersions(response []SubjectVersion) []SubjectVersion {
	subjectMap := map[string]int64{}

	for _, s := range response { // Generate a map of subjects for easier manipulation
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

// Filters the provided map of [ID]:[Subject:Version] according to what is provided in AllowList and DisallowList
func filterIDs(candidate map[int64]map[string][]int64) map[int64]map[string][]int64 {

	for id, subjects := range candidate { // Filter out for allow lists
		for sbj, _ := range subjects {
			if AllowList != nil { // If allow list is defined
				_, allowContains := AllowList[sbj]
				if !allowContains { // If allow list does not contain it, delete it
					delete(candidate[id], sbj)
				}
			}
			if DisallowList != nil { // If disallow list is defined
				_, disallowContains := DisallowList[sbj]
				if disallowContains { // If disallow list contains it, delete it
					delete(candidate[id], sbj)
				}
			}
		}
		if len(subjects) == 0 {
			delete(candidate, id)
		}
	}

	return candidate
}

// Generic handling of queries to the SR instance. Returns back the response as a map of string:string
// Not suitable for endpoint queries that do not conform to this structure
func handleEndpointQuery(end string, src *SchemaRegistryClient) (map[string]string, bool) {
	endpoint := fmt.Sprintf("%s/%s", src.SRUrl, end)
	req := GetNewRequest("GET", endpoint, src.SRApiKey, src.SRApiSecret, nil, nil)

	res, err := httpClient.Do(req)
	if err != nil {
		log.Printf(err.Error())
		return nil, false
	}
	handleNotSuccess(res.Body, res.StatusCode, req.Method, endpoint)

	response := map[string]string{}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf(err.Error())
		return nil, false
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Printf(err.Error())
	}
	return response, true
}

// Returns the difference between the provided maps of Subject:Version
// The difference will be what is contained in the left map that is not contained in the right map
func GetSubjectDiff(m1 map[string][]int64, m2 map[string][]int64) map[string][]int64 {
	diffMap := map[string][]int64{}
	for subject, versions := range m1 {
		if m2[subject] != nil {
			versionDiff := GetVersionsDiff(m1[subject], m2[subject])
			if len(versionDiff) != 0 {
				aDiff := versionDiff
				diffMap[subject] = aDiff
			}
		} else {
			diffMap[subject] = versions
		}
	}
	return diffMap
}

// Returns the difference between the provided slices
// The difference will be what is contained in the left slice that is not contained in the right slice
func GetVersionsDiff(a1 []int64, a2 []int64) []int64 {
	m := map[int64]bool{}
	diff := []int64{}

	for _, item := range a2 {
		m[item] = true
	}

	for _, item := range a1 {
		if _, ok := m[item]; !ok {
			diff = append(diff, item)
		}
	}
	return diff
}

// Returns the difference between the provided maps of [ID][Subject:Version]
// The difference will be what is contained in the left map that is not contained in the right map
func GetIDDiff(m1 map[int64]map[string][]int64, m2 map[int64]map[string][]int64) map[int64]map[string][]int64 {
	diffMap := map[int64]map[string][]int64{}

	for idLeft, subjectVersionsLeftMap := range m1 { // Iterate through the left id -> (subject->version) mapping
		subjVersionsRightMap, idExistsRight := m2[idLeft] // Check if right has this mapping, if it does, retrieve it
		if !idExistsRight {                               // if the right does NOT have this mapping
			diffMap[idLeft] = subjectVersionsLeftMap // This whole mapping gets added to the map of things to be deleted
		} else { // if the right DOES have the ID
			toDelete := map[int64]map[string][]int64{}                      // Holder for schema/version references to delete
			for subjectLeft, versionsLeft := range subjectVersionsLeftMap { // iterate through subject/versions for current id
				subjectRightVersions, subjectExistsRight := subjVersionsRightMap[subjectLeft]
				if subjectExistsRight {
					for _, singleVersionLeft := range versionsLeft { // Iterate through versions on left
						if !isInSlice(singleVersionLeft, subjectRightVersions) { // if not exists on right
							_, idInQueue := toDelete[idLeft]
							if idInQueue {
								_, subjectInQueue := toDelete[idLeft][subjectLeft]
								if subjectInQueue {
									toDelete[idLeft][subjectLeft] = append(toDelete[idLeft][subjectLeft], singleVersionLeft) // Add to holder for queueing for deletion
								} else {
									tmpIDContents := toDelete[idLeft]
									tmpIDContents[subjectLeft] = []int64{singleVersionLeft}
									toDelete[idLeft] = tmpIDContents
								}
							} else {
								tempMap := map[string][]int64{subjectLeft: {singleVersionLeft}}
								toDelete[idLeft] = tempMap
							}
						}
					}
				} else {
					_, idInQueue := toDelete[idLeft]
					if idInQueue {
						tempMap := toDelete[idLeft]
						tempMap[subjectLeft] = versionsLeft
						toDelete[idLeft] = tempMap
					} else {
						toDelete[idLeft] = map[string][]int64{subjectLeft: versionsLeft}
					}
				}
			}
			if len(toDelete) != 0 {
				diffMap[idLeft] = toDelete[idLeft] // Add deletion queue to diffMap
			}
		}
	}

	return diffMap
}

// Returns the currently registered subjects for the SR clients provided
func GetCurrentSubjectsStates(srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) (map[string][]int64, map[string][]int64) {
	return GetCurrentSubjectState(srcClient), GetCurrentSubjectState(destClient)
}

// Returns the currently registered subjects for the single SR provided
func GetCurrentSubjectState(client *SchemaRegistryClient) map[string][]int64 {
	subjects := make(map[string][]int64)
	aChan := make(chan map[string][]int64)

	go client.GetSubjectsWithVersions(aChan, false)

	subjects = <-aChan
	return subjects
}

// Listens for user-provided controlled exit, and terminated the current process
func listenForInterruption() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received %v signal, quitting non-started schema writes...", sig)
		CancelRun = true
	}()
}

func RegisterReferences(wrappingSchema SchemaRecord, srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient, deleted bool) {
	if len(wrappingSchema.References) != 0 {
		log.Printf("Registering references for subject %s and version %d", wrappingSchema.Subject, wrappingSchema.Version)
		for _, schemaReference := range wrappingSchema.References {
			schema := srcClient.GetSchema(schemaReference.Subject, schemaReference.Version, deleted)

			schemaAlreadyRegistered := new(SchemaAlreadyRegisteredResponse)

			responseBody := destClient.RegisterSchemaBySubjectAndIDAndVersion(schema.Schema,
				schema.Subject,
				schema.Id,
				schema.Version,
				schema.SType,
				schema.References)

			err := json.Unmarshal(responseBody, &schemaAlreadyRegistered)

			if err == nil {
				log.Printf("Reference schema subject %s was already written with version: %d and ID: %d", schema.Subject, schema.Version, schema.Id)
			} else {
				log.Printf("Registering referenced schema: %s with version: %d and ID: %d and Type: %s",
					schema.Subject, schema.Version, schema.Id, schema.SType)
			}
		}
	}
}

func RegisterReferencesFromLocalFS(referencesToRegister []SchemaReference, dstClient *SchemaRegistryClient, pathToLookForReferences string) {

	err := filepath.Walk(pathToLookForReferences,
		func(path string, info os.FileInfo, err error) error {
			check(err)
			for _, oneRef := range referencesToRegister {
				if !info.IsDir() && strings.Contains(info.Name(), fmt.Sprintf("%s-%d", url.QueryEscape(oneRef.Subject), oneRef.Version)) {
					log.Println(fmt.Sprintf("Writing referenced schema with Subject: %s and Version: %d. Filepath: %s", oneRef.Subject, oneRef.Version, path))
					writeSchemaToSR(dstClient, path)
				}
			}

			return nil
		})
	check(err)
}

func GetAvroSchemaDescriptor (fullReferenceName string) SchemaDescriptor {

	lastDot := strings.LastIndex(fullReferenceName,".")

	if lastDot == -1 {
		return SchemaDescriptor{}
	}

	namespace := fullReferenceName[:lastDot]
	name := fullReferenceName[lastDot+1:]

	thisDescriptor := SchemaDescriptor{
		namespace: strings.TrimSpace(namespace),
		name:      strings.TrimSpace(name),
	}

	return thisDescriptor
}
