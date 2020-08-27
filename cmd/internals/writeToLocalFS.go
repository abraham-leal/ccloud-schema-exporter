package client

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func WriteToFS (srcClient *SchemaRegistryClient, definedPath string) {

	abspath, _ := os.Executable()
	if definedPath == "" {
		_ = os.Mkdir("SchemaRegistryBackup", 0755)
		definedPath = fmt.Sprintf("%s%s",filepath.Dir(abspath),"/SchemaRegistryBackup")
	}

	srcChan := make(chan map[string][]int)
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

func writeSchema (srcClient *SchemaRegistryClient, pathToWrite string, subject string, version int, wg *sync.WaitGroup) {
	rawSchema := srcClient.GetSchema(subject,int64(version))


	log.Printf("Writing schema: %s with version: %d and ID: %d and Type: %s",
		rawSchema.Subject, rawSchema.Version, rawSchema.Id, rawSchema.SType)

	f , err := os.Create(fmt.Sprintf("%s/%s-%d-%d.json",pathToWrite,rawSchema.Subject,rawSchema.Version,rawSchema.Id))
	check(err)
	defer f.Close()

	_, err = f.WriteString(rawSchema.Schema)
	check(err)
	_ = f.Sync()

	wg.Done()
}

func check (e error){
	if e != nil {
		panic(e)
	}
}