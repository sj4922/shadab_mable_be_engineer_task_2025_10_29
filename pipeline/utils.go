package pipeline

import (
	"os"
	"strconv"
)

// getEnvAsIntOnly retrieves an environment variable as an integer, with a default value.
func getEnvAsIntOnly(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

// getEnvOrDefault returns the environment variable value or the default if not set
func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
