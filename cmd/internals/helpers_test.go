package client

//
// helpers_test.go
// Copyright 2020 Abraham Leal
//

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestMainStackHelpers(t *testing.T) {
	t.Run("TisInSlice", func(t *testing.T) { TisInSlice(t) })
	t.Run("TcheckSubjectIsAllowed", func(t *testing.T) { TcheckSubjectIsAllowed(t) })
	t.Run("TfilterListedSubjects", func(t *testing.T) { TfilterListedSubjects(t) })
	t.Run("TfilterListedSubjectsVersions", func(t *testing.T) { TfilterListedSubjectsVersions(t) })
	t.Run("TfilterIDs", func(t *testing.T) { TfilterIDs(t) })
	t.Run("TGetSubjectDiff", func(t *testing.T) { TGetSubjectDiff(t) })
	t.Run("TGetVersionsDiff", func(t *testing.T) { TGetVersionsDiff(t) })
	t.Run("TGetIDDiff", func(t *testing.T) { TGetIDDiff(t) })
}

func TisInSlice(t *testing.T) {
	sliceOne := []int64{1, 2, 3, 4, 5}

	assert.True(t, isInSlice(3, sliceOne))
	assert.True(t, !isInSlice(6, sliceOne))
}

func TcheckSubjectIsAllowed(t *testing.T) {
	AllowList = map[string]bool{
		"testingSubjectKey": true,
		"SomeOtherValue":    true,
	}

	DisallowList = map[string]bool{
		"someValuePartTwo": true,
		"SomeOtherValue":   true,
	}

	assert.True(t, checkSubjectIsAllowed("testingSubjectKey"))
	assert.True(t, !checkSubjectIsAllowed("SomeOtherValue"))

	AllowList = nil
	DisallowList = nil

}

func TfilterListedSubjects(t *testing.T) {

	AllowList = map[string]bool{
		"testingSubjectKey": true,
		"SomeOtherValue":    true,
	}

	DisallowList = map[string]bool{
		"someValuePartTwo": true,
		"SomeOtherValue":   true,
	}

	toFiler := []string{
		"someValuePartTwo",
		"testingSubjectKey",
		"SomeOtherValue",
	}

	result := filterListedSubjects(toFiler)

	expected := map[string]bool{"testingSubjectKey": true}

	AllowList = nil
	DisallowList = nil

	assert.True(t, reflect.DeepEqual(result, expected))
}

func TfilterListedSubjectsVersions(t *testing.T) {
	AllowList = map[string]bool{
		"testingSubjectKey": true,
		"SomeOtherValue":    true,
	}

	DisallowList = map[string]bool{
		"someValuePartTwo": true,
		"SomeOtherValue":   true,
	}

	toFilter := []SubjectVersion{
		{Subject: "testingSubjectKey", Version: 1},
		{Subject: "someValuePartTwo", Version: 1},
		{Subject: "SomeOtherValue", Version: 1},
	}

	result := filterListedSubjectsVersions(toFilter)

	expected := []SubjectVersion{
		{Subject: "testingSubjectKey", Version: 1},
	}

	AllowList = nil
	DisallowList = nil

	assert.True(t, reflect.DeepEqual(result, expected))

}

func TfilterIDs(t *testing.T) {

	AllowList = map[string]bool{
		"testingSubjectKey": true,
		"SomeOtherValue":    true,
	}

	DisallowList = map[string]bool{
		"someValuePartTwo": true,
		"SomeOtherValue":   true,
	}

	toFilter := map[int64]map[string][]int64{
		1001: {"testingSubjectKey": {1, 2, 3}, "someValuePartTwo": {1, 2}},
		5000: {"SomeOtherValue": {1, 2}},
	}

	result := filterIDs(toFilter)

	expected := map[int64]map[string][]int64{
		1001: {"testingSubjectKey": {1, 2, 3}},
	}

	AllowList = nil
	DisallowList = nil

	assert.True(t, reflect.DeepEqual(result, expected))
}

func TGetSubjectDiff(t *testing.T) {
	subjectMapOne := map[string][]int64{
		"someValue":      {1, 2, 3},
		"SomeOtherValue": {1, 2},
	}

	subjectMapTwo := map[string][]int64{
		"someValue":      {1, 3},
		"SomeOtherValue": {2},
	}

	result := GetSubjectDiff(subjectMapOne, subjectMapTwo)

	expected := map[string][]int64{
		"someValue":      {2},
		"SomeOtherValue": {1},
	}

	// Assert values that are contained in left but not right are returned
	assert.True(t, reflect.DeepEqual(expected, result))
}

func TGetVersionsDiff(t *testing.T) {
	versionArrayOne := []int64{1, 2, 3}
	versionArrayTwo := []int64{1, 3}

	result := GetVersionsDiff(versionArrayOne, versionArrayTwo)

	expected := []int64{2}

	// Assert values that are contained in left but not right are returned
	assert.Equal(t, expected, result)
}

func TGetIDDiff(t *testing.T) {

	idMapOne := map[int64]map[string][]int64{
		1001: {"someValue": {1, 2, 3}, "someValuePartTwo": {1, 2}},
		5000: {"SomeOtherValue": {1, 2}},
	}

	idMapTwo := map[int64]map[string][]int64{
		1001: {"someValue": {1, 3}, "someValuePartTwo": {1}},
		5000: {"SomeOtherValue": {2}},
	}

	results := GetIDDiff(idMapOne, idMapTwo)

	expected := map[int64]map[string][]int64{
		1001: {"someValue": {2}, "someValuePartTwo": {2}},
		5000: {"SomeOtherValue": {1}},
	}

	assert.True(t, reflect.DeepEqual(expected, results))
}
