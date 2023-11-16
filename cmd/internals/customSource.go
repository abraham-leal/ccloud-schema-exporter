package client

//
// customSource.go
// Copyright 2020 Abraham Leal
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Runner for the custom source sync mode
func RunCustomSourceSync(dstClient *SchemaRegistryClient, customSrc CustomSource) {
	err := customSrc.SetUp()
	if err != nil {
		log.Println("Could not perform proper set-up of custom source")
		log.Println(err)
	}

	listenForInterruption()

	//Begin sync
	for {
		if CancelRun == true {
			err := customSrc.TearDown()
			if err != nil {
				log.Println("Could not perform proper tear-down of custom source")
				log.Println(err)
			}
			return
		}
		beginSync := time.Now()

		srcSubjects, err := customSrc.GetSourceState()
		destSubjects := GetCurrentSubjectState(dstClient)
		checkDontFail(err)

		if !reflect.DeepEqual(srcSubjects, destSubjects) {
			diff := GetSubjectDiff(srcSubjects, destSubjects)
			// Perform sync
			customSrcSync(diff, dstClient, customSrc)
			//We anticipate that the custom source will not have the concept of hard or soft deletes
			if SyncDeletes {
				customSrcSyncDeletes(destSubjects, srcSubjects, dstClient, customSrc)
			}
		}
		syncDuration := time.Since(beginSync)
		log.Printf("Finished sync in %d ms", syncDuration.Milliseconds())

		time.Sleep(time.Duration(ScrapeInterval) * time.Second)
	}

	return
}

// Sync job for custom source
func customSrcSync(diff map[string][]int64, dstClient *SchemaRegistryClient, customSrc CustomSource) {
	if len(diff) != 0 {
		log.Println("Custom Source has values that Schema Registry does not, syncing...")
		for subject, versions := range diff {
			for _, v := range versions {
				id, stype, schema, references, err := customSrc.GetSchema(subject, v)
				if err != nil {
					log.Println("Could not retrieve schema from custom source")
				}
				if checkSubjectIsAllowed(subject) {
					thisSchemaRecord := SchemaRecord{
						Subject:    subject,
						Schema:     schema,
						SType:      stype,
						Version:    v,
						Id:         id,
						References: references,
					}
					// Assure References are registered first
					RegisterReferencesWithCustomSource(thisSchemaRecord, customSrc, dstClient)
					log.Println("Registering new schema: " + subject +
						" with version: " + strconv.FormatInt(v, 10) +
						" and ID: " + strconv.FormatInt(id, 10) +
						" and Type: " + stype)
					dstClient.RegisterSchemaBySubjectAndIDAndVersion(thisSchemaRecord.Schema,
						thisSchemaRecord.Subject, thisSchemaRecord.Id, thisSchemaRecord.Version,
						thisSchemaRecord.SType, thisSchemaRecord.References)
				}
			}
		}
	}
}

// Sync deletes for custom source, this performs hard deleted in the Schema Registry
func customSrcSyncDeletes(destSubjects map[string][]int64, srcSubjects map[string][]int64, dstClient *SchemaRegistryClient, customSrc CustomSource) {
	diff := GetSubjectDiff(destSubjects, srcSubjects)
	if len(diff) != 0 {
		log.Println("Source registry has deletes that Destination does not, syncing...")
		for sbj, versions := range diff {
			for _, v := range versions {
				if checkSubjectIsAllowed(sbj) {
					if dstClient.subjectExists(sbj) {
						if dstClient.PerformSoftDelete(sbj, v) {
							dstClient.PerformHardDelete(sbj, v)
						}
					}
				}
			}
		}
	}
}

// Runner for the batch job of custom source
func RunCustomSourceBatch(dstClient *SchemaRegistryClient, customSrc CustomSource) {
	err := customSrc.SetUp()
	if err != nil {
		log.Println("Could not perform proper set-up of custom source")
		log.Println(err)
	}

	listenForInterruption()

	srcSubjects, err := customSrc.GetSourceState()
	checkDontFail(err)

	log.Println("Registering all schemas from custom source")
	for subject, srcVersions := range srcSubjects {
		if CancelRun == true {
			err := customSrc.TearDown()
			if err != nil {
				log.Println("Could not perform proper tear-down of custom source")
				log.Println(err)
			}
			return
		}
		for _, v := range srcVersions {
			id, stype, schema, references, err := customSrc.GetSchema(subject, v)
			if err != nil {
				log.Println("Could not retrieve schema from custom source")
			} else {
				if checkSubjectIsAllowed(subject) {
					thisSchemaRecord := SchemaRecord{
						Subject:    subject,
						Schema:     schema,
						SType:      stype,
						Version:    v,
						Id:         id,
						References: references,
					}
					// Assure references are registered first
					RegisterReferencesWithCustomSource(thisSchemaRecord, customSrc, dstClient)
					log.Printf("Registering schema: %s with version: %d and ID: %d and Type: %s",
						subject, v, id, stype)
					dstClient.RegisterSchemaBySubjectAndIDAndVersion(thisSchemaRecord.Schema,
						thisSchemaRecord.Subject, thisSchemaRecord.Id, thisSchemaRecord.Version,
						thisSchemaRecord.SType, thisSchemaRecord.References)
				}
			}
		}
	}
}

// Registers the schema references given in the SchemaRecord, recursively, for a custom source
func RegisterReferencesWithCustomSource(wrappingSchema SchemaRecord, customSrc CustomSource, destClient *SchemaRegistryClient) {
	if len(wrappingSchema.References) != 0 {
		log.Printf("Registering references for subject %s and version %d", wrappingSchema.Subject, wrappingSchema.Version)
		for _, schemaReference := range wrappingSchema.References {
			schemaId, schemaType, schemaString, schemaReferencesWithin, err := customSrc.GetSchema(schemaReference.Subject, schemaReference.Version)
			if err != nil {
				log.Println("Could not retrieve schema from custom source")
			}
			if len(schemaReferencesWithin) != 0 {
				thisReferenceSchemaRecord := SchemaRecord{
					Subject:    schemaReference.Subject,
					Schema:     schemaString,
					SType:      schemaType,
					Version:    schemaReference.Version,
					Id:         schemaId,
					References: schemaReferencesWithin,
				}
				RegisterReferencesWithCustomSource(thisReferenceSchemaRecord, customSrc, destClient)
			}

			schemaAlreadyRegistered := new(SchemaAlreadyRegisteredResponse)

			responseBody := destClient.RegisterSchemaBySubjectAndIDAndVersion(schemaString,
				schemaReference.Subject,
				schemaId,
				schemaReference.Version,
				schemaType,
				schemaReferencesWithin)

			err = json.Unmarshal(responseBody, &schemaAlreadyRegistered)

			if err == nil {
				log.Printf("Reference schema subject %s was already written with version: %d and ID: %d",
					schemaReference.Subject, schemaReference.Version, schemaId)
			} else {
				log.Printf("Registering referenced schema: %s with version: %d and ID: %d and Type: %s",
					schemaReference.Subject, schemaReference.Version, schemaId, schemaType)
			}
		}
	}
}

/*
This is an example of a custom source.
This example uses Apicurio Registry as the source.
*/
func NewApicurioSource() ApicurioSource {
	apicurioOptionsVar := os.Getenv("APICURIO_OPTIONS")
	apicurioOptionsMap := map[string]string{}
	if apicurioOptionsVar != "" {
		tempOptionsSlice := strings.Split(apicurioOptionsVar, ";")
		for _, option := range tempOptionsSlice {
			splitOption := strings.SplitN(option, "=", 2)
			apicurioOptionsMap[splitOption[0]] = splitOption[1]
		}
		log.Printf("Starting Apicurio Source with endpoint: %s", apicurioOptionsMap["apicurioUrl"])
		return ApicurioSource{
			Options:     apicurioOptionsMap,
			apiCurioUrl: apicurioOptionsMap["apicurioUrl"],
		}
	}
	return ApicurioSource{
		Options:     apicurioOptionsMap,
		apiCurioUrl: "http://localhost:8081",
	}
}

// In-Mem Custom Source for testing purposes
func NewInMemRegistry(records []SchemaRecord) inMemRegistry {
	state := map[int64]SchemaRecord{}
	for _, record := range records {
		state[record.Id] = SchemaRecord{
			Subject:    record.Subject,
			Schema:     record.Schema,
			SType:      record.SType,
			Version:    record.Version,
			Id:         record.Id,
			References: record.References,
		}
	}

	return inMemRegistry{state}
}

/*
Implementation of a simple custom source
*/
type inMemRegistry struct {
	inMemSchemas map[int64]SchemaRecord
}

func (iM inMemRegistry) SetUp() error {
	return nil
}

func (iM inMemRegistry) GetSchema(sbj string, version int64) (id int64, stype string, schema string, references []SchemaReference, err error) {
	for _, schemaRecord := range iM.inMemSchemas {
		if schemaRecord.Subject == sbj && schemaRecord.Version == version {
			return schemaRecord.Id, schemaRecord.SType, schemaRecord.Schema, schemaRecord.References, nil
		}
	}
	return 0, "", "", nil, fmt.Errorf("schema not found")
}

func (iM inMemRegistry) GetSourceState() (map[string][]int64, error) {
	currentState := map[string][]int64{}
	for _, schemaRecord := range iM.inMemSchemas {
		_, haveSeen := currentState[schemaRecord.Subject]
		if haveSeen {
			currentState[schemaRecord.Subject] = append(currentState[schemaRecord.Subject], schemaRecord.Version)
		} else {
			currentState[schemaRecord.Subject] = []int64{schemaRecord.Version}
		}
	}
	return currentState, nil
}

func (iM inMemRegistry) TearDown() error {
	return nil
}

// Another example of a custom source

type SchemaApicurioMeta struct {
	Name       string            `json:"name"`
	CreatedOn  int64             `json:"createdOn,omitempty"`
	ModifiedOn int64             `json:"modifiedOn,omitempty"`
	Id         string            `json:"id,omitempty"`
	Version    int64             `json:"version"`
	Stype      string            `json:"type"`
	GlobalId   int64             `json:"globalId"`
	State      string            `json:"state,omitempty"`
	Labels     []string          `json:"labels,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type ApicurioSource struct {
	Options                              map[string]string
	apiCurioUrl                          string
	apiCurioCoreRegistryAPIv1            string
	apiCurioCoreRegistryAPIv2            string
	apiCurioConfluentSchemaRegistryAPIv6 string
}

func (ap *ApicurioSource) SetUp() error {
	url, exists := ap.Options["apicurioUrl"]
	if exists && ap.apiCurioUrl != "http://localhost:8081" {
		ap.apiCurioUrl = url
		ap.apiCurioCoreRegistryAPIv1 = url + "/apis/registry/v1"
		ap.apiCurioCoreRegistryAPIv2 = url + "/apis/registry/v2"
		ap.apiCurioConfluentSchemaRegistryAPIv6 = url + "/apis/ccompat/v6"
		delete(ap.Options, "apicurioUrl")
	} else {
		log.Println("Options not provided, using local apicurio connection at: http://localhost:8081")
	}
	return nil
}

func (ap *ApicurioSource) GetSchema(subject string, version int64) (id int64, stype string, schema string, references []SchemaReference, err error) {
	log.Printf("Getting schema for subject: %s and version: %d", subject, version)
	getSchemaEndpoint := fmt.Sprintf("%s/subjects/%s/versions/%d", ap.apiCurioConfluentSchemaRegistryAPIv6, url.QueryEscape(subject), version)
	schemaReq := GetNewRequest("GET", getSchemaEndpoint, "x", "x", ap.Options, nil)

	schemaResponse, err := httpClient.Do(schemaReq)
	if schemaResponse.StatusCode != 200 {
		log.Println("Could not fetch schema")
	}

	schemaRefsBody, err := ioutil.ReadAll(schemaResponse.Body)
	checkDontFail(err)
	schemaResponse.Body.Close()

	schemaRefsContainer := SchemaToRegister{}
	err = json.Unmarshal(schemaRefsBody, &schemaRefsContainer)

	return schemaRefsContainer.Id, schemaRefsContainer.SType, schemaRefsContainer.Schema, schemaRefsContainer.References, nil
}

func (ap *ApicurioSource) GetSourceState() (map[string][]int64, error) {
	// Get All Artifacts
	listArtifactsEndpoint := fmt.Sprintf("%s/artifacts", ap.apiCurioCoreRegistryAPIv1)
	listReq := GetNewRequest("GET", listArtifactsEndpoint, "x", "x", ap.Options, nil)
	listResponse, err := httpClient.Do(listReq)
	checkDontFail(err)
	if listResponse.StatusCode != 200 {
		log.Println("Could not fetch artifact metadata for state assessment")
	}

	listResponseContainer := []string{}
	listBody, err := ioutil.ReadAll(listResponse.Body)
	checkDontFail(err)
	listResponse.Body.Close()
	err = json.Unmarshal(listBody, &listResponseContainer)
	checkDontFail(err)

	// Get All Versions
	ArtifactVersionMap := map[string][]int64{}
	for _, artifactID := range listResponseContainer {
		listArtifactsVersionsEndpoint := fmt.Sprintf("%s/%s/versions", listArtifactsEndpoint, artifactID)
		versionsReq := GetNewRequest("GET", listArtifactsVersionsEndpoint, "x", "x", ap.Options, nil)
		listVersionResponse, err := httpClient.Do(versionsReq)
		checkDontFail(err)
		if listVersionResponse.StatusCode != 200 {
			log.Println("Could not fetch version metadata for state assessment")
		}
		versionsResponseContainer := []int64{}
		versionsBody, err := ioutil.ReadAll(listVersionResponse.Body)
		checkDontFail(err)
		listResponse.Body.Close()
		err = json.Unmarshal(versionsBody, &versionsResponseContainer)
		checkDontFail(err)

		ArtifactVersionMap[artifactID] = versionsResponseContainer
	}

	sourceState := map[string][]int64{}
	// Get All necessary metadata
	for artifactID, versions := range ArtifactVersionMap {
		for _, version := range versions {
			listArtifactsVersionsMetaEndpoint := fmt.Sprintf("%s/%s/versions/%v/meta", listArtifactsEndpoint, artifactID, version)
			metaReq := GetNewRequest("GET", listArtifactsVersionsMetaEndpoint, "x", "x", ap.Options, nil)

			metaResponse, err := httpClient.Do(metaReq)
			checkDontFail(err)
			if metaResponse.StatusCode != 200 {
				log.Println("Could not fetch schema metadata for state assessment")
			}
			metaResponseContainer := SchemaApicurioMeta{}
			metaBody, err := ioutil.ReadAll(metaResponse.Body)
			checkDontFail(err)
			metaResponse.Body.Close()
			err = json.Unmarshal(metaBody, &metaResponseContainer)
			checkDontFail(err)

			if metaResponseContainer.Stype == "AVRO" || metaResponseContainer.Stype == "JSON" ||
				metaResponseContainer.Stype == "PROTOBUF" {
				artifactVersions, haveSeenBefore := sourceState[metaResponseContainer.Id]
				if !haveSeenBefore {
					sourceState[metaResponseContainer.Id] = []int64{metaResponseContainer.Version}
				} else {
					artifactVersions := append(artifactVersions, metaResponseContainer.Version)
					sourceState[metaResponseContainer.Id] = artifactVersions
				}
			}
		}
	}

	return sourceState, nil
}

func (ap *ApicurioSource) TearDown() error {
	return nil
}
