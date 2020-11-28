package client

//
// writeToLocal.go
// Copyright 2020 Abraham Leal
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

func WriteToFS(srcClient *SchemaRegistryClient, definedPath string, workingDirectory string) {
	listenForInterruption()

	definedPath = CheckPath(definedPath, workingDirectory)

	srcSubjects := GetCurrentSubjectState(srcClient)
	var aGroup sync.WaitGroup

	log.Printf("Writing all schemas from %s to path %s", srcClient.SRUrl, definedPath)
	for srcSubject, srcVersions := range srcSubjects {
		for _, v := range srcVersions {
			aGroup.Add(1)
			go writeSchemaLocally(srcClient, definedPath, srcSubject, v, &aGroup)
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
		log.Println("Destination Schema Registry Fully Restored From Backup")
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
		check(err)
		log.Printf("Registering Schema with Subject: %s. Version: %v, and ID: %v", subject, version, id)
		dstClient.RegisterSchemaBySubjectAndIDAndVersion(string(rawSchema), subject, id, version, stype)
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

	filename := fmt.Sprintf("%s-%d-%d-%s", rawSchema.Subject, rawSchema.Version, rawSchema.Id, rawSchema.SType)
	f, err := os.Create(filepath.Join(pathToWrite, filename))

	check(err)
	defer f.Close()

	_, err = f.WriteString(rawSchema.Schema)
	check(err)
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
