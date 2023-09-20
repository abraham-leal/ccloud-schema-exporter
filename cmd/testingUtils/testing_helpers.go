package testingUtils

import (
	"context"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"net/http"
	"time"
)

var cpTestVersion = "7.4.1"
var Ctx = context.Background()

func GetBaseInfra(networkName string) (kafkaContainer testcontainers.Container, schemaRegistryContainer testcontainers.Container) {
	var network = testcontainers.NetworkRequest{
		Name:   networkName,
		Driver: "bridge",
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := provider.GetNetwork(Ctx, network); err != nil {
		if _, err := provider.CreateNetwork(Ctx, network); err != nil {
			log.Fatal(err)
		}
	}

	kafkaContainer, err = testcontainers.GenericContainer(Ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "confluentinc/confluent-local:" + cpTestVersion,
				ExposedPorts: []string{"29092/tcp", "29093/tcp", "9092/tcp"},
				WaitingFor:   wait.ForListeningPort("29092/tcp"),
				Name:         "broker" + networkName,
				Env: map[string]string{
					"CLUSTER_ID":                                        "E__VgOY5Tna5qbyDTtFbTg",
					"KAFKA_PROCESS_ROLES":                               "broker,controller",
					"KAFKA_NODE_ID":                                     "1",
					"KAFKA_CONTROLLER_QUORUM_VOTERS":                    "1@broker" + networkName + ":29093",
					"KAFKA_CONTROLLER_LISTENER_NAMES":                   "CONTROLLER",
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":              "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
					"KAFKA_LISTENERS":                                   "PLAINTEXT://broker" + networkName + ":29092,CONTROLLER://broker" + networkName + ":29093,PLAINTEXT_HOST://0.0.0.0:9092",
					"KAFKA_ADVERTISED_LISTENERS":                        "PLAINTEXT://broker" + networkName + ":29092, PLAINTEXT_HOST://localhost:9092",
					"KAFKA_INTER_BROKER_LISTENER_NAME":                  "PLAINTEXT",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":            "1",
					"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":            "0",
					"KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR":  "1",
					"KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR": "1",
					"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":               "1",
					"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":    "1",
				},
				Networks: []string{networkName},
			},
			Started: true,
		})
	CheckFail(err, "Kafka was not able to start")

	schemaRegistryContainer, err = testcontainers.GenericContainer(Ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "confluentinc/cp-schema-registry:" + cpTestVersion,
				ExposedPorts: []string{"8081/tcp"},
				WaitingFor:   GetSRWaitStrategy("8081"),
				Name:         "schema-registry-src-" + networkName,
				Env: map[string]string{
					"SCHEMA_REGISTRY_HOST_NAME":                           "schema-registry-src",
					"SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID":            "schema-src",
					"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS":        "broker" + networkName + ":29092",
					"SCHEMA_REGISTRY_KAFKASTORE_TOPIC":                    "_schemas",
					"SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR": "1",
					"SCHEMA_REGISTRY_MODE_MUTABILITY":                     "true",
				},
				Networks: []string{networkName},
			},
			Started: true,
		})
	CheckFail(err, "Source SR was not able to start")

	return kafkaContainer, schemaRegistryContainer
}

// Pass in: Port to wait for 200 return
func GetSRWaitStrategy(port string) wait.Strategy {
	var i int
	return wait.ForHTTP("/").
		WithPort(nat.Port(port + "/tcp")).
		WithStartupTimeout(time.Second * 30).
		WithMethod(http.MethodGet).
		WithStatusCodeMatcher(func(status int) bool { i++; return i > 1 && status == 200 })
}

// Simple check function that will fail all if there is an error present and allows a custom message to be printed
func CheckFail(e error, msg string) {
	if e != nil {
		log.Println(e)
		log.Fatalln(msg)
	}
}
