package client

import (
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"
)

var mutex = sync.Mutex{}
var srcIDs = map[int][]SubjectVersion{}
var dstIDs = map[int][]SubjectVersion{}

func Sync (srcClient *SchemaRegistryClient, destClient *SchemaRegistryClient) {

	//Begin sync
	for {
		if TestHarnessRun == true {
			return
		}
		beginSync := time.Now()

		srcSubjects := make (map[string][]int64)
		destSubjects := make (map[string][]int64)

		srcChan := make(chan map[string][]int64)
		destChan := make(chan map[string][]int64)

		go srcClient.GetSubjectsWithVersions(srcChan)
		go destClient.GetSubjectsWithVersions(destChan)

		srcSubjects = <- srcChan
		destSubjects = <- destChan

		// Perform SR Sync without worrying about deletes
		if !reflect.DeepEqual(srcSubjects,destSubjects) {
			diff := GetSubjectDiff(srcSubjects, destSubjects)
			if len(diff) != 0 {
				log.Println("Source registry has values that Destination does not, syncing...")
				for subject, versions := range diff {
					for _, v := range versions {
						log.Printf("Subject: %s, Version: %d", subject, v)
						schema := srcClient.GetSchema(subject, int64(v))
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

			//Perform SR sync taking into account soft and hard deletes
			if SyncDeletes {
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
		}

		if SyncHardDeletes {
			permDel := getPermanentlyDeletedSchemas(srcClient,destClient)
			if len(permDel) != 0 {
				for id , subjectVersionMap := range permDel {
					for subject, version := range subjectVersionMap {
						log.Printf("Discovered Hard Deleted Schema with ID %d, Subject %s, and Version: %d",
							id,subject,version)
						destClient.PerformHardDelete(subject, version)
					}
				}
			}
		}

		syncDuration := time.Since(beginSync)
		log.Printf("Finished sync in %d ms", syncDuration.Milliseconds())

		time.Sleep(time.Duration(ScrapeInterval) * time.Second)
	}

}

func GetSubjectDiff ( m1 map[string][]int64, m2 map[string][]int64) map[string][]int64 {
	diffMap := map[string][]int64{}
	for subject, versions := range m1 {
		if m2[subject] != nil {
			versionDiff := GetVersionsDiff(m1[subject],m2[subject])
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

func GetVersionsDiff (a1 []int64, a2 []int64) []int64 {
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

func getPermanentlyDeletedSchemas (src *SchemaRegistryClient, dest *SchemaRegistryClient) map[int64]map[string]int64 {

	aChan := make(chan map[int64]map[string]int64)
	bChan := make(chan map[int64]map[string]int64)
	srcIDs := make(map[int64]map[string]int64)
	destIDs := make(map[int64]map[string]int64)

	go src.GetAllIDs(aChan)
	go dest.GetAllIDs(bChan)

	srcIDs = <- aChan
	destIDs = <- bChan

	return getIDDiff(srcIDs,destIDs)

}

func getIDDiff (m1 map[int64]map[string]int64, m2 map[int64]map[string]int64) map[int64]map[string]int64 {
	diffMap := map[int64]map[string]int64{}

	for idDest, subjectValDestMap := range m2 { // Iterate through destination id -> (subject->version) mapping
		subjValSrcMap , idExistsSrc := m1[idDest] // Check if source has this mapping, if it does, retrieve it
		if !idExistsSrc { // if the source does NOT have this mapping
			diffMap[idDest] = subjectValDestMap // This whole mapping gets added to the map of things to be deleted
		} else { // if the source DOES have the ID
			toDelete := map[string]int64{} // Holder for schema/version references to delete
			for subDest, verDest := range subjectValDestMap { // iterate through subject/versions for current id
				_, verSrcExists := subjValSrcMap[subDest] // check if they exist in source
				if !verSrcExists { // if not exists
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
