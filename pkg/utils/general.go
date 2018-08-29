package utils

import (
	"math/rand"
	"strconv"
	"time"
)

func RandString() string {
	randNum := strconv.Itoa((rand.New(rand.NewSource(time.Now().UnixNano()))).Int())
	b := make([]byte, 6)
	for i := range b {
		b[i] = randNum[rand.Intn(len(randNum))]
	}
	return string(b)
}

func GetTag() string {
	return time.Now().Format("20060102150405")
}
