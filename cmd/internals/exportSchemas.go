package client

import (
	"log"
)

func BatchExport (srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {

	srcChan := make(chan map[string][]int64)
	go srcClient.GetSubjectsWithVersions(srcChan)
	srcSubjects := <- srcChan

	log.Println("Registering all schemas from " + srcClient.SRUrl)
	for srcSubject , srcVersions := range srcSubjects {
		for _ , v := range srcVersions {
			schema := srcClient.GetSchema(srcSubject,int64(v))
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
