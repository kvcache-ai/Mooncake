package common

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

func LoadEnv(envName, defaultEnv string) string {
	value := os.Getenv(envName)
	if value == "" {
		slog.Warn("environment variable is not set, using default value", "envName", envName, "defaultValue", defaultEnv)
		return defaultEnv
	}
	return value
}

func LoadIntEnv(envName string, defaultEnv int) int {
	value := os.Getenv(envName)
	trimmedValue := strings.TrimSpace(value)
	if value != "" {
		intValue, err := strconv.Atoi(value)
		if err != nil {
			slog.Error("invalid value for environment variable", "envName", envName, "value", trimmedValue)
		} else {
			return intValue
		}
	}
	slog.Warn("environment variable is not set, using default value", "envName", envName, "defaultValue", defaultEnv)
	return defaultEnv
}

func ExtractTokenIdFromRequest(data map[string]interface{}, key string) ([]int32, error) {
	raw, exists := data[key]
	if !exists {
		return nil, fmt.Errorf("missing key: %s", key)
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("the value of %s is not an array", key)
	}
	result := make([]int32, len(arr))
	for i, v := range arr {
		switch val := v.(type) {
		case float64:
			result[i] = int32(val)
		case int:
			result[i] = int32(val)
		default:
			return nil, fmt.Errorf("unsupported value type of token_id at [%d], the type is %s", i, val)
		}
	}
	return result, nil
}

func ExtractCandidateEngineFromRequest(data map[string]interface{}, key string) (map[string]struct{}, error) {
	raw, exists := data[key]
	if !exists {
		return nil, fmt.Errorf("missing key: %s", key)
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("the value of %s is not an array", key)
	}
	result := make(map[string]struct{}, len(arr))
	for i, v := range arr {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf(`"instances[%d]" is not a string`, i)
		}
		result[str] = struct{}{}
	}
	return result, nil
}

func ExtractStringValueFromRequest(data map[string]interface{}, key string) (string, error) {
	raw, exists := data[key]
	if !exists {
		return "", fmt.Errorf("missing key: %s", key)
	}
	str, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf(`"the value of: %s" is not a string`, key)
	}
	return str, nil
}

func ExtractIntFromRequest(data map[string]interface{}, key string) (int64, error) {
	raw, exists := data[key]
	if !exists {
		return -1, fmt.Errorf("missing key: %s", key)
	}
	result, ok := raw.(float64)
	if !ok {
		return -1, fmt.Errorf(`"the value of: %s" is not a number`, key)
	}
	return int64(result), nil
}
