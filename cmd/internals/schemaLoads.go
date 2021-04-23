package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type SchemaLoader struct {
	dstClient		*SchemaRegistryClient
	schemasType		SchemaType			// Define the Loader Type
	schemaRecords	map[SchemaDescriptor]map[int64]map[string]interface{}	// Internal map of SchemaDescriptor -> version -> unstructured schema
}

type SchemaDescriptor struct {
	namespace	string
	name		string
}

// Define SchemaType enum
type SchemaType int

const (
	AVRO SchemaType = iota
	PROTOBUF
	JSON
)

func (st SchemaType) String() string {
	return [...]string{"AVRO", "PROTOBUF", "JSON"}[st]
}

// Types that do not need further resolution
var nativeTypes = map[string]struct{}{
"null" :{}, "boolean" : {}, "int" : {}, "long" : {}, "float" : {}, "double" : {} , "bytes" : {} ,
"string" : {}, "record" : {}, "enum" : {}, "array" : {}, "map" : {}, "decimal" : {}, "fixed" : {}, "uuid" : {},
"date" : {}, "time-millis" : {}, "time-micros" : {}, "timestamp-millis" : {}, "timestamp-micros" : {},
"local-timestamp-millis" : {}, "local-timestamp-micros" : {}, "duration" : {},
}

/*
	A SchemaLoader takes in a SchemaType and a path and will load the schemas in the path onto Schema Registry.
	Schema Loaders take in the schemas in their natural form, and registers them, this means that if the underlying
	schema
 */
func NewSchemaLoader (schemaType SchemaType, dstClient *SchemaRegistryClient) *SchemaLoader {
	if schemaType == AVRO {
		return &SchemaLoader{
			dstClient:		dstClient,
			schemasType:  	AVRO,
			schemaRecords: 	map[SchemaDescriptor]map[int64]map[string]interface{}{},
		}
	} else if schemaType == PROTOBUF {
		log.Fatalln("The Protobuf schema load is not supported yet.")
	} else if schemaType == JSON {
		log.Fatalln("The Json schema load is not supported yet.")
	}

	log.Fatalln("This type of schema load is not supported, and there are no plans for support.")
	return nil
}

func (sl *SchemaLoader) run (dstClient *SchemaRegistryClient, givenPath string, workingDirectory string) {
	sl.loadFromPath(givenPath, workingDirectory)

	if sl.schemasType == AVRO {
		for schemaDesc, schemaVersions := range sl.schemaRecords {
			for schemaVersion, fullSchema := range schemaVersions {
				sl.registerAvroSchema(schemaDesc, schemaVersion, fullSchema)
			}
		}
	}

	log.Println("All schemas from given directory are registered!")
	log.Println("Thanks for using ccloud-schema-exporter!")
}

func (sl *SchemaLoader) loadFromPath (givenPath string, workingDirectory string) {
	definedPath := CheckPath(givenPath,workingDirectory)

	listenForInterruption()

	if sl.schemasType == AVRO {
		err := filepath.WalkDir(definedPath, sl.loadAvroFiles)
		check(err)
	}

	if CancelRun != true {
		log.Println("Successfully read schemas")
	} else {
		log.Println("Interrupted while reading schemas")
	}

}

func (sl *SchemaLoader) registerAvroSchema (desc SchemaDescriptor, schemaVer int64, fullSchema map[string]interface{}) bool {

	thisSchemaReferences := []SchemaReference{}

	// Check there are fields to the schema
	if fullSchema["fields"] != nil {
		thisSchemaReferences = sl.getReferencesForAvroSchema(fullSchema["fields"], "type")
		thisSchemaReferences = append(thisSchemaReferences, sl.getReferencesForAvroSchema(fullSchema["fields"], "values")...)
		thisSchemaReferences = append(thisSchemaReferences, sl.getReferencesForAvroSchema(fullSchema["fields"], "items")...)
	} else {
		log.Fatalln("Could not determine fields in schema!")
	}

	if !sl.dstClient.(desc.namespace+"."+desc.name) {
		sl.dstClient.RegisterSchema(
			fmt.Sprintf("%v", fullSchema),
			desc.namespace+"."+desc.name,
			"AVRO",
			thisSchemaReferences,
		)
	}



	return true
}

func (sl *SchemaLoader) loadAvroFiles(path string, info os.DirEntry, err error) error {
	check(err)
	
	if !info.IsDir() {
		currentSchema, err := os.Open(path)
		checkDontFail(err)
		defer currentSchema.Close()

		jsonBytes, _ := ioutil.ReadAll(currentSchema)

		var schemaStruct map[string]interface{}
		json.Unmarshal(jsonBytes, &schemaStruct)
		
		thisSchemaDescription := SchemaDescriptor{
			namespace: fmt.Sprintf("%v", schemaStruct["namespace"]),
			name:      fmt.Sprintf("%v", schemaStruct["name"]),
		}

		thisSchemaVersion := int64(len(sl.schemaRecords[thisSchemaDescription]))

		if thisSchemaVersion == 0 {
			sl.schemaRecords[thisSchemaDescription] = map[int64]map[string]interface{}{
				thisSchemaVersion: schemaStruct,
			}
		} else {
			newVersion := sl.schemaRecords[thisSchemaDescription]
			newVersion[thisSchemaVersion] = schemaStruct
			sl.schemaRecords[thisSchemaDescription] = newVersion
		}

	}
	return nil
}

func (sl *SchemaLoader) getReferencesForAvroSchema (fields interface{}, fieldToSwitch string) []SchemaReference {
	references := []SchemaReference{}

	switch theseFields := fields.(type) {
	case []map[string]interface{}:
		for _, onefield := range theseFields {
			switch typeOfTypeField := onefield[fieldToSwitch].(type) {
			case string:
				_, exists := nativeTypes[typeOfTypeField]
				if !exists {
					// If this is a reference, then fill array and register that first
					//Build SchemaDescriptor for the reference
					thisReferenceDescriptor := GetAvroSchemaDescriptor(typeOfTypeField)

					versions, refExists := sl.schemaRecords[thisReferenceDescriptor]
					if !refExists {
						log.Fatalln("Reference doesn't exist: " + typeOfTypeField)
					}
					latestVersionForReference := int64(len(versions))

					sl.registerAvroSchema(thisReferenceDescriptor, latestVersionForReference, sl.schemaRecords[thisReferenceDescriptor][latestVersionForReference])

					references = append(references, SchemaReference{
						Name:    typeOfTypeField,			// The type referenced
						Subject: typeOfTypeField+"-value", 	// The SchemaDescriptor
						Version: latestVersionForReference, 		// Latest version of schema descriptor
					})

				}
			case []string:
				for _,oneOfType := range typeOfTypeField {
					_, exists := nativeTypes[oneOfType]
					if !exists {
						// If this is a reference, then fill array and register that first
						//Build SchemaDescriptor for the reference
						thisReferenceDescriptor := GetAvroSchemaDescriptor(oneOfType)

						versions, refExists := sl.schemaRecords[thisReferenceDescriptor]
						if !refExists {
							log.Fatalln("Reference doesn't exist: " + oneOfType)
						}
						latestVersionForReference := int64(len(versions))

						sl.registerAvroSchema(thisReferenceDescriptor, latestVersionForReference, sl.schemaRecords[thisReferenceDescriptor][latestVersionForReference])

						references = append(references, SchemaReference{
							Name:    oneOfType,			// The type referenced
							Subject: oneOfType+"-value", 	// The SchemaDescriptor
							Version: latestVersionForReference, 		// Latest version of schema descriptor
						})
					}
				}
			default:
				// Do nothing
			}
		}

	default:
		log.Println("Could not define the fields in this schema")
	}

	return references
}

