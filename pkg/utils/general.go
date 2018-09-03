package utils

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// SplitInTwo splits a string to two parts by a delimeter
func SplitInTwo(s, sep string) (string, string) {
	if !strings.Contains(s, sep) {
		log.Fatal(s, "does not contain", sep)
	}
	split := strings.Split(s, sep)
	return split[0], split[1]
}

// GetRandString returns a randon string
func GetRandString() string {
	randNum := strconv.Itoa((rand.New(rand.NewSource(time.Now().UnixNano()))).Int())
	b := make([]byte, 6)
	for i := range b {
		b[i] = randNum[rand.Intn(len(randNum))]
	}
	return string(b)
}

// GetTimeStamp returns time stamp
func GetTimeStamp() string {
	return time.Now().Format("20060102150405")
}

// MapKeysToSlice converts a map to a slice using the keys as the values
func MapKeysToSlice(m map[string]string) []string {
	var slice []string
	for k := range m {
		slice = append(slice, k)
	}
	return slice
}

// SliceContainsSlice verifies that outer slice contains inner slice
func SliceContainsSlice(outers, inners []string) error {
	for _, outer := range outers {
		found := false
		for _, inner := range inners {
			if outer == inner {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%s not contained in %s", outer, inners)
		}
	}
	return nil
}

// Contains checks if a slice contains a given value
func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
