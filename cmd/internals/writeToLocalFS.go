package client

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func WriteToFS (srcClient *SchemaRegistryClient, definedPath string) {

	definedPath = CheckPath(definedPath)

	srcChan := make(chan map[string][]int64)
	go srcClient.GetSubjectsWithVersions(srcChan)
	srcSubjects := <- srcChan
	var aGroup sync.WaitGroup

	log.Printf("Writing all schemas from %s to path %s", srcClient.SRUrl, definedPath)
	for srcSubject , srcVersions := range srcSubjects {
		for _ , v := range srcVersions {
			aGroup.Add(1)
			go writeSchema(srcClient, definedPath, srcSubject, v, &aGroup)
		}
	}
	aGroup.Wait()
}

func writeSchema (srcClient *SchemaRegistryClient, pathToWrite string, subject string, version int64, wg *sync.WaitGroup) {
	rawSchema := srcClient.GetSchema(subject,int64(version))


	log.Printf("Writing schema: %s with version: %d and ID: %d",
		rawSchema.Subject, rawSchema.Version, rawSchema.Id)

	filename := fmt.Sprintf("%s-%d-%d",rawSchema.Subject, rawSchema.Version, rawSchema.Id)
	f , err := os.Create(filepath.Join(pathToWrite,filename))

	check(err)
	defer f.Close()

	_, err = f.WriteString(rawSchema.Schema)
	check(err)
	_ = f.Sync()

	wg.Done()
}

func CheckPath (definedPath string) string {

	currentPath, _ := os.Getwd()

	if definedPath == "" {
		log.Println("Path not defined, writing to new local folder SchemaRegistryBackup")
		_ = os.Mkdir("SchemaRegistryBackup", 0755)
		definedPath = filepath.Join(filepath.Dir(currentPath), "SchemaRegistryBackup")
		return definedPath
	} else {
		if filepath.IsAbs(definedPath){
			if _, err := os.Stat(definedPath); os.IsNotExist(err) {
				log.Println("Path: " + definedPath)
				log.Fatalln("The directory specified does not exist.")
			}
		} else {
			definedPath = filepath.Join(filepath.Dir(currentPath),definedPath)
			_, err := os.Stat(definedPath)
			if os.IsNotExist(err) {
				log.Println("Path: " + definedPath)
				log.Fatalln("The directory specified does not exist.")
			}
		}
		return definedPath
	}
}

func check (e error){
	if e != nil {
		panic(e)
	}
}