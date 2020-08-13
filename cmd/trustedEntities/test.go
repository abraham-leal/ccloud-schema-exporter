package main

import (
	"fmt"
	"reflect"
)

func main() {

	a := map[string][]int {"hello" : {1}}
	b := map[string][]int {"hello" : {1,2}}

	if (reflect.DeepEqual(a,b)){
		fmt.Println("These are DeepEqual")
	} else {
		fmt.Println("These are NOT DeepEqual")
	}

}
