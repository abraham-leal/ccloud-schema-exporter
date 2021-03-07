package client

//
// writeToLocal.go
// Copyright 2020 Abraham Leal
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func WriteToFS(srcClient *SchemaRegistryClient, definedPath string, workingDirectory string) {
	listenForInterruption()

	definedPath = CheckPath(definedPath, workingDirectory)

	srcSubjects := GetCurrentSubjectState(srcClient)
	var aGroup sync.WaitGroup

	log.Printf("Writing schemas from %s to path %s", srcClient.SRUrl, definedPath)
	for srcSubject, srcVersions := range srcSubjects {
		for _, v := range srcVersions {
			aGroup.Add(1)
			go writeSchemaLocally(srcClient, definedPath, srcSubject, v, &aGroup)
			time.Sleep(time.Duration(1) * time.Millisecond)
		}
	}
	aGroup.Wait()
}

func WriteFromFS(dstClient *SchemaRegistryClient, definedPath string, workingDirectory string) {
	listenForInterruption()

	definedPath = CheckPath(definedPath, workingDirectory)

	err := filepath.Walk(definedPath,
		func(path string, info os.FileInfo, err error) error {
			check(err)
			if !info.IsDir() {
				writeSchemaToSR(dstClient, path)
			}
			return nil
		})
	check(err)

	if CancelRun != true {
		log.Println("Destination Schema Registry Restored From Backup")
	} else {
		log.Println("Destination Schema Registry Partially Restored From Backup")
	}
}

// Writes the provided file to Schema Registry
func writeSchemaToSR(dstClient *SchemaRegistryClient, filepath string) {
	if CancelRun == true {
		return
	}
	id, version, subject, stype := parseFileName(filepath)
	if checkSubjectIsAllowed(subject) {
		rawSchema, err := ioutil.ReadFile(filepath)
		fileString := string(rawSchema)
		check(err)

		referenceArray := []SchemaReference{}

		if strings.Contains(fileString, ReferenceSeparator) {
			referenceStart := strings.LastIndex(fileString, ReferenceSeparator) + len(ReferenceSeparator)
			referenceString := strings.TrimSpace(strings.ReplaceAll(fileString[referenceStart:], "\n", ""))
			referenceCollection := strings.Split(referenceString, "|")
			for _, reference := range referenceCollection {
				if len(reference) != 0 {
					thisReference := SchemaReference{}
					err := json.Unmarshal([]byte(reference), &thisReference)
					check(err)
					referenceArray = append(referenceArray, thisReference)
				}
			}
			RegisterReferencesFromLocalFS(referenceArray, dstClient, path.Dir(filepath))
			fileString = fileString[:strings.LastIndex(fileString, ReferenceSeparator)-1]
		}

		unescapedSubject, err := url.QueryUnescape(subject)
		checkDontFail(err)
		log.Printf("Registering Schema with Subject: %s. Version: %v, and ID: %v", unescapedSubject, version, id)
		dstClient.RegisterSchemaBySubjectAndIDAndVersion(fileString, unescapedSubject, id, version, stype, referenceArray)
	}
}

// Returns schema metadata for a file given its path
// Returned metadata in order: SchemaID, SchemaVersion, SchemaSubject, SchemaType
func parseFileName(filepath string) (int64, int64, string, string) {

	startFileName := 0
	if strings.LastIndex(filepath, "/") != -1 {
		startFileName = strings.LastIndex(filepath, "/") + 1
	}
	schemaFileName := filepath[startFileName:]
	schemaType := schemaFileName[strings.LastIndex(schemaFileName, "-")+1:]
	schemaFileNameNoType := schemaFileName[:strings.LastIndex(schemaFileName, "-")]
	schemaId, err := strconv.ParseInt(schemaFileNameNoType[strings.LastIndex(schemaFileNameNoType, "-")+1:], 10, 64)
	check(err)
	schemaFileNameNoId := schemaFileNameNoType[:strings.LastIndex(schemaFileNameNoType, "-")]
	schemaVersion, err := strconv.ParseInt(schemaFileNameNoId[strings.LastIndex(schemaFileNameNoId, "-")+1:], 10, 64)
	check(err)
	schemaSubject := schemaFileNameNoId[:strings.LastIndex(schemaFileNameNoId, "-")]

	return schemaId, schemaVersion, schemaSubject, schemaType
}

// Writes the provided schema in the given path
func writeSchemaLocally(srcClient *SchemaRegistryClient, pathToWrite string, subject string, version int64, wg *sync.WaitGroup) {
	rawSchema := srcClient.GetSchema(subject, version, false)
	defer wg.Done()
	if CancelRun == true {
		return
	}

	log.Printf("Writing schema: %s with version: %d and ID: %d",
		rawSchema.Subject, rawSchema.Version, rawSchema.Id)

	filename := fmt.Sprintf("%s-%d-%d-%s", url.QueryEscape(rawSchema.Subject), rawSchema.Version, rawSchema.Id, rawSchema.SType)
	f, err := os.Create(filepath.Join(pathToWrite, filename))

	check(err)
	defer f.Close()

	_, err = f.WriteString(rawSchema.Schema)
	check(err)
	if len(rawSchema.References) != 0 {
		_, err = f.WriteString("\n")
		check(err)
		_, err = f.WriteString(ReferenceSeparator)
		check(err)
		_, err = f.WriteString("\n")
		check(err)
		for _, oneRef := range rawSchema.References {
			jsonRepresentation, err := json.Marshal(oneRef)
			check(err)
			_, err = f.Write(jsonRepresentation)
			check(err)
			_, err = f.WriteString("|\n")
			check(err)
		}
	}

	_ = f.Sync()
}

// Returns a valid local FS path to write the schemas to
func CheckPath(definedPath string, workingDirectory string) string {

	currentPath := filepath.Clean(workingDirectory)

	if definedPath == "" {
		definedPath = filepath.Join(currentPath, "SchemaRegistryBackup")
		log.Println("Path not defined, using local folder SchemaRegistryBackup")
		_ = os.Mkdir(definedPath, 0755)
		return definedPath
	} else {
		if filepath.IsAbs(definedPath) {
			if _, err := os.Stat(definedPath); os.IsNotExist(err) {
				log.Println("Path: " + definedPath)
				log.Fatalln("The directory specified does not exist.")
			}
		} else {
			definedPath = filepath.Join(currentPath, definedPath)
			_, err := os.Stat(definedPath)
			if os.IsNotExist(err) {
				log.Println("Path: " + definedPath)
				log.Fatalln("The directory specified does not exist.")
			}
		}
		return definedPath
	}
}

// Registers the given references by looking them up in the path given.
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
