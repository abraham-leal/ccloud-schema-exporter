package client

//
// customDestination.go
// Copyright 2020 Abraham Leal
//

import (
	"log"
	"reflect"
	"strconv"
	"time"
)

func RunCustomDestinationSync(srcClient *SchemaRegistryClient, customDest CustomDestination) {
	err := customDest.SetUp()
	if err != nil {
		log.Println("Could not perform proper set-up of custom destination")
		log.Println(err)
	}

	listenForInterruption()

	//Begin sync
	for {
		if CancelRun == true {
			err := customDest.TearDown()
			if err != nil {
				log.Println("Could not perform proper tear-down of custom destination")
				log.Println(err)
			}
			return
		}
		beginSync := time.Now()

		srcSubjects := GetCurrentSubjectState(srcClient)
		destSubjects, err := customDest.GetDestinationState()
		checkDontFail(err)

		if !reflect.DeepEqual(srcSubjects, destSubjects) {
			diff := GetSubjectDiff(srcSubjects, destSubjects)
			// Perform sync
			customDestSync(diff, srcClient, customDest)
			//We anticipate that the custom destination will not have the notion of hard or soft deletes
			if SyncDeletes {
				customDestSyncDeletes(destSubjects, srcSubjects, srcClient, customDest)
			}
		}
		syncDuration := time.Since(beginSync)
		log.Printf("Finished sync in %d ms", syncDuration.Milliseconds())

		time.Sleep(time.Duration(ScrapeInterval) * time.Second)
	}
}

func RunCustomDestinationBatch(srcClient *SchemaRegistryClient, customDest CustomDestination) {
	err := customDest.SetUp()
	if err != nil {
		log.Println("Could not perform proper set-up of custom destination")
		log.Println(err)
	}

	listenForInterruption()

	srcChan := make(chan map[string][]int64)
	go srcClient.GetSubjectsWithVersions(srcChan, false)
	srcSubjects := <-srcChan

	log.Println("Registering all schemas from " + srcClient.SRUrl)
	for srcSubject, srcVersions := range srcSubjects {
		if CancelRun == true {
			err := customDest.TearDown()
			if err != nil {
				log.Println("Could not perform proper tear-down of custom destination")
				log.Println(err)
			}
			return
		}
		for _, v := range srcVersions {
			schema := srcClient.GetSchema(srcSubject, v, false)
			log.Printf("Registering schema: %s with version: %d and ID: %d and Type: %s",
				schema.Subject, schema.Version, schema.Id, schema.SType)
			err := customDest.RegisterSchema(schema)
			checkCouldNotRegister(err)
		}
	}
}

func customDestSync(diff map[string][]int64, srcClient *SchemaRegistryClient, customDest CustomDestination) {
	if len(diff) != 0 {
		log.Println("Source registry has values that Destination does not, syncing...")
		for subject, versions := range diff {
			for _, v := range versions {
				schema := srcClient.GetSchema(subject, v, false)
				log.Println("Registering new schema: " + schema.Subject +
					" with version: " + strconv.FormatInt(schema.Version, 10) +
					" and ID: " + strconv.FormatInt(schema.Id, 10) +
					" and Type: " + schema.SType)
				err := customDest.RegisterSchema(schema)
				checkCouldNotRegister(err)
			}
		}
	}
}

func customDestSyncDeletes(destSubjects map[string][]int64, srcSubjects map[string][]int64, srcClient *SchemaRegistryClient, customDest CustomDestination) {
	diff := GetSubjectDiff(destSubjects, srcSubjects)
	if len(diff) != 0 {
		log.Println("Source registry has deletes that Destination does not, syncing...")
		for subject, versions := range diff {
			for _, v := range versions {
				schema := srcClient.GetSchema(subject, v, false)
				err := customDest.DeleteSchema(schema)
				checkCouldNotRegister(err)
			}
		}
	}
}

/*
This is a simple example of implementing the CustomDestination interface.
It holds schemas in memory and performs/reports all necessary calls.
*/

type SampleCustomDestination struct {
	inMemState map[string][]int64
}

func NewSampleCustomDestination() SampleCustomDestination {
	return SampleCustomDestination{inMemState: map[string][]int64{}}
}

func (cd *SampleCustomDestination) SetUp() error {
	// Nothing to set up
	return nil
}

func (cd *SampleCustomDestination) RegisterSchema(record SchemaRecord) error {
	currentVersionSlice, exists := cd.inMemState[record.Subject]
	if exists {
		tempVersionSlice := append(currentVersionSlice, record.Version)
		cd.inMemState[record.Subject] = tempVersionSlice

	} else {
		tempVersionSlice := []int64{record.Version}
		cd.inMemState[record.Subject] = tempVersionSlice
	}
	return nil
}

func (cd *SampleCustomDestination) DeleteSchema(record SchemaRecord) error {
	currentVersionSlice, exists := cd.inMemState[record.Subject]
	newSlice := currentVersionSlice
	if exists {
		for index, v := range currentVersionSlice {
			if v == record.Version {
				newSlice = removeFromSlice(currentVersionSlice, index)
			}
		}
		cd.inMemState[record.Subject] = newSlice
	}
	return nil
}

func (cd *SampleCustomDestination) GetDestinationState() (map[string][]int64, error) {
	return cd.inMemState, nil
}

func (cd *SampleCustomDestination) TearDown() error {
	// Nothing to tear-down
	return nil
}

func removeFromSlice(s []int64, i int) []int64 {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func checkCouldNotRegister(err error) {
	if err != nil {
		log.Println("Could not register schema to destination:")
		log.Println(err)
	}
}
