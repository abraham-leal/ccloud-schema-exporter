package client

//
// writeToLocal.go
// Author: Abraham Leal
//

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
)

func WriteToFS (srcClient *SchemaRegistryClient, definedPath string, workingDirectory string) {
	// Listen for program interruption
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received %v signal, quitting non-started schema writes...", sig)
		CancelRun = true
	}()

	definedPath = CheckPath(definedPath, workingDirectory)

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
	rawSchema := srcClient.GetSchema(subject,version)
	defer wg.Done()
	if CancelRun == true {
		return
	}


	log.Printf("Writing schema: %s with version: %d and ID: %d",
		rawSchema.Subject, rawSchema.Version, rawSchema.Id)

	filename := fmt.Sprintf("%s-%d-%d",rawSchema.Subject, rawSchema.Version, rawSchema.Id)
	f , err := os.Create(filepath.Join(pathToWrite,filename))

	check(err)
	defer f.Close()

	_, err = f.WriteString(rawSchema.Schema)
	check(err)
	_ = f.Sync()
}

func CheckPath (definedPath string, workingDirectory string) string {

	currentPath := filepath.Clean(workingDirectory)

	if definedPath == "" {
		definedPath = filepath.Join(currentPath, "SchemaRegistryBackup")
		log.Println("Path not defined, writing to new local folder SchemaRegistryBackup")
		_ = os.Mkdir(definedPath, 0755)
		return definedPath
	} else {
		if filepath.IsAbs(definedPath){
			if _, err := os.Stat(definedPath); os.IsNotExist(err) {
				log.Println("Path: " + definedPath)
				log.Fatalln("The directory specified does not exist.")
			}
		} else {
			definedPath = filepath.Join(currentPath,definedPath)
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