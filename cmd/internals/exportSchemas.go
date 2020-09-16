package client

//
// exportSchemas.go
// Author: Abraham Leal
//

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func BatchExport (srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {
	// Listen for program interruption
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received %v signal, finishing current subject and quitting...", sig)
		CancelRun = true
	}()


	srcChan := make(chan map[string][]int64)
	go srcClient.GetSubjectsWithVersions(srcChan)
	srcSubjects := <- srcChan

	log.Println("Registering all schemas from " + srcClient.SRUrl)
	for srcSubject , srcVersions := range srcSubjects {
		if CancelRun == true {
			return
		}
		for _ , v := range srcVersions {
			schema := srcClient.GetSchema(srcSubject,v)
			log.Printf("Registering schema: %s with version: %d and ID: %d and Type: %s", 
			schema.Subject, schema.Version, schema.Id, schema.SType)
			destClient.RegisterSchemaBySubjectAndIDAndVersion(schema.Schema,
				schema.Subject,
				schema.Id,
				schema.Version,
				schema.SType)
		}
	}
}
