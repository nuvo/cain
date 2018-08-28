package utils

import (
	"time"
)

func GetTag() string {
	return time.Now().Format("20060102150405")
}
