package client

//
// getInfo.go
// Copyright 2020 Abraham Leal
//

import (
	"errors"
	"os"
)

// SrcGetAPIKey returns the API Key from environment variables
// if an API Key can not be found, it exits the process
func SrcGetAPIKey() string {
	key, present := os.LookupEnv("SRC_API_KEY")
	if present && key != "" {
		return key
	}

	panic(errors.New("SRC_API_KEY environment variable has not been specified"))
}

// SrcGetAPISecret returns the API Key from environment variables
// if an API Key can not be found, it exits the process
func SrcGetAPISecret() string {
	secret, present := os.LookupEnv("SRC_API_SECRET")
	if present && secret != "" {
		return secret
	}

	panic(errors.New("SRC_API_SECRET environment variable has not been specified"))
}

// SrcGetSRUrl returns the Source URL from environment variables
// if an API Key can not be found, it exits the process
func SrcGetSRUrl() string {
	url, present := os.LookupEnv("SRC_SR_URL")
	if present && url != "" {
		return url
	}

	panic(errors.New("SRC_SR_URL environment variable has not been specified"))
}

// DestGetAPIKey returns the API Key from environment variables
// if an API Key can not be found, it exits the process
func DestGetAPIKey() string {
	key, present := os.LookupEnv("DST_API_KEY")
	if present && key != "" {
		return key
	}

	panic(errors.New("DST_API_KEY environment variable has not been specified"))
}

// DestGetAPISecret returns the API Key from environment variables
// if an API Key can not be found, it exits the process
func DestGetAPISecret() string {
	secret, present := os.LookupEnv("DST_API_SECRET")
	if present && secret != "" {
		return secret
	}

	panic(errors.New("DST_API_SECRET environment variable has not been specified"))
}

// DestGetSRUrl returns the Destination URL from environment variables
// if a URL can not be found, it exits the process
func DestGetSRUrl() string {
	url, present := os.LookupEnv("DST_SR_URL")
	if present && url != "" {
		return url
	}

	panic(errors.New("DST_SR_URL environment variable has not been specified"))
}

// DestGetSRContext returns the Destination Context from environment variables
// if a Context can not be found, it exits the process
func DestGetSRContext() string {
	context, present := os.LookupEnv("DST_SR_CONTEXT")
	if present && context != "" {
		return context
	} else {
		return "default"
	}
}
