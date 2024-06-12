package utils

import (
	"os"
	"strconv"
	"strings"
)

// GetIntEnvVar returns 0 if the variable is empty or not int, else the value
func GetIntEnvVar(name string, defVal int) int {
	val := os.Getenv(name)
	if val == "" {
		return defVal
	}
	iVal, err := strconv.Atoi(val)
	if err != nil {
		return defVal
	}
	return iVal
}

// GetStringEnvVar returns the default value if the variable is empty, else the value
func GetStringEnvVar(name, defVal string) string {
	val := os.Getenv(name)
	if val == "" {
		return defVal
	}
	return val
}

// GetBoolEnvVar returns the default value if the variable is empty or not true or false, else the value
func GetBoolEnvVar(name string, defVal bool) bool {
	val := os.Getenv(name)
	if strings.ToLower(val) == "true" {
		return true
	}
	if strings.ToLower(val) == "false" {
		return false
	}
	return defVal
}

// GetFloat64EnvVar returns the default value if the variable is empty, else the value
func GetFloat64EnvVar(name string, defVal float64) float64 {
	val := os.Getenv(name)
	if val == "" {
		return defVal
	}
	iVal, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return defVal
	}
	return iVal
}

// GetInt64EnvVar returns the default value if the variable is empty, else the value
func GetInt64EnvVar(name string, defVal int64) int64 {
	val := os.Getenv(name)
	if val == "" {
		return defVal
	}
	iVal, err := strconv.ParseInt(val, 0, 64)
	if err != nil {
		return defVal
	}
	return iVal
}
