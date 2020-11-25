package client

//
// syncSchemas.go
// Author: Abraham Leal
//

import (
	"log"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"syscall"
	"time"
)

func Sync(srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {

	// Listen for program interruption
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received %v signal, finishing current sync and quitting...", sig)
		CancelRun = true
	}()

	//Begin sync
	for {
		if CancelRun == true {
			return
		}
		beginSync := time.Now()

		srcSubjects, destSubjects := getCurrentSubjectsStates(srcClient, destClient)

		if !reflect.DeepEqual(srcSubjects, destSubjects) {
			diff := GetSubjectDiff(srcSubjects, destSubjects)
			// Perform sync
			initialSync(diff, srcClient, destClient)
			//Perform soft delete check
			if SyncDeletes {
				syncSoftDeletes(destSubjects, srcSubjects, destClient)
			}
		}

		// Perform hard delete check
		if SyncHardDeletes {
			syncHardDeletes(srcClient, destClient)
		}

		syncDuration := time.Since(beginSync)
		log.Printf("Finished sync in %d ms", syncDuration.Milliseconds())

		time.Sleep(time.Duration(ScrapeInterval) * time.Second)
	}

}

func initialSync(diff map[string][]int64, srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {
	if len(diff) != 0 {
		log.Println("Source registry has values that Destination does not, syncing...")
		for subject, versions := range diff {
			for _, v := range versions {
				schema := srcClient.GetSchema(subject, v)
				log.Println("Registering new schema: " + schema.Subject +
					" with version: " + strconv.FormatInt(schema.Version, 10) +
					" and ID: " + strconv.FormatInt(schema.Id, 10) +
					" and Type: " + schema.SType)
				destClient.RegisterSchemaBySubjectAndIDAndVersion(schema.Schema,
					schema.Subject,
					schema.Id,
					schema.Version,
					schema.SType)
			}
		}
	}
}

func syncSoftDeletes(destSubjects map[string][]int64, srcSubjects map[string][]int64, destClient *SchemaRegistryClient) {
	diff := GetSubjectDiff(destSubjects, srcSubjects)
	if len(diff) != 0 {
		log.Println("Source registry has deletes that Destination does not, syncing...")
		for subject, versions := range diff {
			for _, v := range versions {
				destClient.PerformSoftDelete(subject, v)
			}
		}
	}
}

func syncHardDeletes(srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {
	permDel := getIDDiff(srcClient.GetSoftDeletedIDs(), destClient.GetSoftDeletedIDs())
	if len(permDel) != 0 {
		for id, subjectVersionMap := range permDel {
			for subject, version := range subjectVersionMap {
				log.Printf("Discovered Hard Deleted Schema with ID %d, Subject %s, and Version: %d",
					id, subject, version)
				destClient.PerformHardDelete(subject, version)
			}
		}
	}
}
