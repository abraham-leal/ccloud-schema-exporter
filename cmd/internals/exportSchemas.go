package client

//
// exportSchemas.go
// Author: Abraham Leal
//

import (
	"log"
)

func BatchExport(srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {
	listenForInterruption()

	srcChan := make(chan map[string][]int64)
	go srcClient.GetSubjectsWithVersions(srcChan)
	srcSubjects := <-srcChan

	log.Println("Registering all schemas from " + srcClient.SRUrl)
	for srcSubject, srcVersions := range srcSubjects {
		if CancelRun == true {
			return
		}
		for _, v := range srcVersions {
			schema := srcClient.GetSchema(srcSubject, v)
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
