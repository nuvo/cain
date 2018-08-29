package utils

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

func GetRandString() string {
	randNum := strconv.Itoa((rand.New(rand.NewSource(time.Now().UnixNano()))).Int())
	b := make([]byte, 6)
	for i := range b {
		b[i] = randNum[rand.Intn(len(randNum))]
	}
	return string(b)
}

func GetTimeStamp() string {
	return time.Now().Format("20060102150405")
}

func MapKeysToSlice(m map[string]string) []string {
	var slice []string
	for k := range m {
		slice = append(slice, k)
	}
	return slice
}

func Contains(outers, inners []string) error {
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
