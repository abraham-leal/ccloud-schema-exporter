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

		srcSubjects := make(map[string][]int64)
		destSubjects := make(map[string][]int64)

		srcChan := make(chan map[string][]int64)
		destChan := make(chan map[string][]int64)

		go srcClient.GetSubjectsWithVersions(srcChan)
		go destClient.GetSubjectsWithVersions(destChan)

		srcSubjects = <-srcChan
		destSubjects = <-destChan

		log.Println(srcSubjects)
		log.Println(destSubjects)

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
	permDel := getPermanentlyDeletedSchemas(srcClient, destClient)
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

func GetSubjectDiff(m1 map[string][]int64, m2 map[string][]int64) map[string][]int64 {
	diffMap := map[string][]int64{}
	for subject, versions := range m1 {
		if m2[subject] != nil {
			versionDiff := GetVersionsDiff(m1[subject], m2[subject])
			if len(versionDiff) != 0 {
				aDiff := versionDiff
				diffMap[subject] = aDiff
			}
		} else {
			diffMap[subject] = versions
		}
	}
	return diffMap
}

func GetVersionsDiff(a1 []int64, a2 []int64) []int64 {
	m := map[int64]bool{}
	diff := []int64{}

	for _, item := range a2 {
		m[item] = true
	}

	for _, item := range a1 {
		if _, ok := m[item]; !ok {
			diff = append(diff, item)
		}
	}
	return diff
}

func getPermanentlyDeletedSchemas(src *SchemaRegistryClient, dest *SchemaRegistryClient) map[int64]map[string]int64 {
	return getIDDiff(src.GetSoftDeletedIDs(), dest.GetSoftDeletedIDs())
}

func getIDDiff(m1 map[int64]map[string]int64, m2 map[int64]map[string]int64) map[int64]map[string]int64 {
	diffMap := map[int64]map[string]int64{}

	for idDest, subjectValDestMap := range m2 { // Iterate through destination id -> (subject->version) mapping
		subjValSrcMap, idExistsSrc := m1[idDest] // Check if source has this mapping, if it does, retrieve it
		if !idExistsSrc {                        // if the source does NOT have this mapping
			diffMap[idDest] = subjectValDestMap // This whole mapping gets added to the map of things to be deleted
		} else { // if the source DOES have the ID
			toDelete := map[string]int64{}                    // Holder for schema/version references to delete
			for subDest, verDest := range subjectValDestMap { // iterate through subject/versions for current id
				_, verSrcExists := subjValSrcMap[subDest] // check if they exist in source
				if !verSrcExists {                        // if not exists
					toDelete[subDest] = verDest // Add to holder for queueing for deletion
				}
			}
			if len(toDelete) != 0 {
				diffMap[idDest] = toDelete // Add deletion queue to diffMap
			}
		}
	}

	return diffMap
}
