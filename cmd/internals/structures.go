package client

//
// structures.go
// Author: Abraham Leal
//

type SchemaRegistryClient struct {
	SRUrl string
	SRApiKey string
	SRApiSecret string
}

type SchemaStringResponse struct {
	Subject string `json:"subject"`
	Schema string  `json:"schema"`
	SType string   `json:"schemaType"`
	Version int64  `json:"version"`
	Id int64	   `json:"id"`
}

//Constructor to assure Type-less schemas get registered with Avro
func (srs SchemaStringResponse) setTypeIfEmpty () SchemaStringResponse {
	if (srs.SType == ""){
		srs.SType = "AVRO"
	}

	return srs
}

type SchemaToRegister struct {
	Schema string 	`json:"schema"`
	Id int 			`json:"id"`
	Version int 	`json:"version"`
	SType string 	`json:"schemaType"`
}

type SchemaExtraction struct {
	Schema string 	`json:"schema"`
	Id int 			`json:"id"`
	Version int 	`json:"version"`
	Subject string 	`json:"subject"`
}

type ImportMode struct {
	Mode string 	`json:"mode"`
}