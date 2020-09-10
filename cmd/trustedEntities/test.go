package main

import (
	"flag"
	"fmt"
	client "github.com/abraham-leal/ccloud-schema-exporter/cmd/internals"
	"io/ioutil"
	"strings"
	"unicode"
)

type StringArrayFlag []string

func (i *StringArrayFlag) String() string {
	return fmt.Sprintln(*i)
}

func (i *StringArrayFlag) Set(value string) error {

	if strings.LastIndexAny(value,"/.") != -1 {
		path := client.CheckPath(value)
		f , err := ioutil.ReadFile(path)
		if err != nil {
			panic(err)
		}
		value = string(f)
	}

	nospaces := i.removeSpaces(value)
	*i = strings.Split(nospaces,",")
	return nil
}

func (i *StringArrayFlag) removeSpaces (str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}

var myFlags StringArrayFlag

func main() {
	flag.Var(&myFlags, "list1", "Some description for this param.")
	flag.Parse()

	fmt.Println(myFlags)

}
