package common

import (
	"log/slog"
	"os"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	// test default log level
	origLevel := os.Getenv("CONDUCTOR_LOG_LEVEL")
	os.Unsetenv("CONDUCTOR_LOG_LEVEL")
	defer os.Setenv("CONDUCTOR_LOG_LEVEL", origLevel)

	level := ParseLogLevel()
	if level != 0 { // slog.LevelInfo = 0
		t.Errorf("Expected default log level to be Info, got %d", level)
	}

	// test various log levels
	testCases := []struct {
		name     string
		levelStr string
		expected slog.Level
	}{
		{"Debug", "DEBUG", slog.LevelDebug},
		{"Info", "INFO", slog.LevelInfo},
		{"Warn", "WARN", slog.LevelWarn},
		{"Error", "ERROR", slog.LevelError},
		{"Lowercase", "debug", slog.LevelDebug},
		{"MixedCase", "Debug", slog.LevelDebug},
		{"Invalid", "INVALID", slog.LevelInfo},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			os.Setenv("CONDUCTOR_LOG_LEVEL", tc.levelStr)
			level := ParseLogLevel()
			if level != tc.expected {
				t.Errorf("Expected log level %d for input %s, got %d", tc.expected, tc.levelStr, level)
			}
		})
	}
}

func TestLoadEnv(t *testing.T) {
	// test environment variable exists
	origValue := os.Getenv("TEST_ENV_VAR")
	os.Setenv("TEST_ENV_VAR", "test_value")
	defer func() {
		if origValue == "" {
			os.Unsetenv("TEST_ENV_VAR")
		} else {
			os.Setenv("TEST_ENV_VAR", origValue)
		}
	}()

	value := LoadEnv("TEST_ENV_VAR", "default_value")
	if value != "test_value" {
		t.Errorf("Expected LoadEnv to return 'test_value', got '%s'", value)
	}

	// test environment variable does not exist
	origValue2 := os.Getenv("NON_EXISTENT_ENV_VAR")
	os.Unsetenv("NON_EXISTENT_ENV_VAR")
	defer func() {
		if origValue2 != "" {
			os.Setenv("NON_EXISTENT_ENV_VAR", origValue2)
		}
	}()

	value = LoadEnv("NON_EXISTENT_ENV_VAR", "default_value")
	if value != "default_value" {
		t.Errorf("Expected LoadEnv to return 'default_value', got '%s'", value)
	}
}

func TestLoadIntEnv(t *testing.T) {
	// test environment variable exists and value is valid
	origValue := os.Getenv("TEST_INT_ENV_VAR")
	os.Setenv("TEST_INT_ENV_VAR", "42")
	defer func() {
		if origValue == "" {
			os.Unsetenv("TEST_INT_ENV_VAR")
		} else {
			os.Setenv("TEST_INT_ENV_VAR", origValue)
		}
	}()

	value := LoadIntEnv("TEST_INT_ENV_VAR", 100)
	if value != 42 {
		t.Errorf("Expected LoadIntEnv to return 42, got %d", value)
	}

	// test environment variable exists but value is invalid
	os.Setenv("TEST_INT_ENV_VAR", "invalid")
	value = LoadIntEnv("TEST_INT_ENV_VAR", 100)
	if value != 100 {
		t.Errorf("Expected LoadIntEnv to return 100 for invalid value, got %d", value)
	}

	// test environment variable does not exist
	origValue2 := os.Getenv("NON_EXISTENT_INT_ENV_VAR")
	os.Unsetenv("NON_EXISTENT_INT_ENV_VAR")
	defer func() {
		if origValue2 != "" {
			os.Setenv("NON_EXISTENT_INT_ENV_VAR", origValue2)
		}
	}()

	value = LoadIntEnv("NON_EXISTENT_INT_ENV_VAR", 100)
	if value != 100 {
		t.Errorf("Expected LoadIntEnv to return 100 for non-existent env, got %d", value)
	}
}

func TestExtractTokenIdFromRequest(t *testing.T) {
	// test successful extraction
	data := map[string]interface{}{
		"token_ids": []interface{}{1.0, 2.0, 3.0},
	}
	result, err := ExtractTokenIdFromRequest(data, "token_ids")
	if err != nil {
		t.Errorf("Expected ExtractTokenIdFromRequest to succeed, got error: %v", err)
	}
	expected := []int32{1, 2, 3}
	if len(result) != len(expected) {
		t.Errorf("Expected length %d, got %d", len(expected), len(result))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("Expected result[%d] = %d, got %d", i, expected[i], v)
		}
	}

	// test mixed number types
	data["token_ids"] = []interface{}{1, 2.0, 3}
	result, err = ExtractTokenIdFromRequest(data, "token_ids")
	if err != nil {
		t.Errorf("Expected ExtractTokenIdFromRequest to succeed with mixed number types, got error: %v", err)
	}
	expected = []int32{1, 2, 3}
	if len(result) != len(expected) {
		t.Errorf("Expected length %d, got %d", len(expected), len(result))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("Expected result[%d] = %d, got %d", i, expected[i], v)
		}
	}

	// test missing key
	result, err = ExtractTokenIdFromRequest(data, "missing_key")
	if err == nil {
		t.Errorf("Expected ExtractTokenIdFromRequest to fail with missing key, got success")
	}

	// test non-array value
	data["token_ids"] = "not an array"
	result, err = ExtractTokenIdFromRequest(data, "token_ids")
	if err == nil {
		t.Errorf("Expected ExtractTokenIdFromRequest to fail with non-array value, got success")
	}

	// test unsupported types
	data["token_ids"] = []interface{}{"string", true}
	result, err = ExtractTokenIdFromRequest(data, "token_ids")
	if err == nil {
		t.Errorf("Expected ExtractTokenIdFromRequest to fail with unsupported types, got success")
	}
}

func TestExtractCandidateEngineFromRequest(t *testing.T) {
	// test successful extraction
	data := map[string]interface{}{
		"instances": []interface{}{"engine1", "engine2", "engine3"},
	}
	result, err := ExtractCandidateEngineFromRequest(data, "instances")
	if err != nil {
		t.Errorf("Expected ExtractCandidateEngineFromRequest to succeed, got error: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 engines, got %d", len(result))
	}
	expectedEngines := []string{"engine1", "engine2", "engine3"}
	for _, engine := range expectedEngines {
		if _, ok := result[engine]; !ok {
			t.Errorf("Expected engine '%s' not found in result", engine)
		}
	}

	// test missing key
	result, err = ExtractCandidateEngineFromRequest(data, "missing_key")
	if err == nil {
		t.Errorf("Expected ExtractCandidateEngineFromRequest to fail with missing key, got success")
	}

	// test non-array value
	data["instances"] = "not an array"
	result, err = ExtractCandidateEngineFromRequest(data, "instances")
	if err == nil {
		t.Errorf("Expected ExtractCandidateEngineFromRequest to fail with non-array value, got success")
	}

	// test non-string element
	data["instances"] = []interface{}{"engine1", 2, "engine3"}
	result, err = ExtractCandidateEngineFromRequest(data, "instances")
	if err == nil {
		t.Errorf("Expected ExtractCandidateEngineFromRequest to fail with non-string element, got success")
	}
}

func TestExtractStringValueFromRequest(t *testing.T) {
	// test successful extraction
	data := map[string]interface{}{
		"str_key": "string_value",
	}
	result, err := ExtractStringValueFromRequest(data, "str_key")
	if err != nil {
		t.Errorf("Expected ExtractStringValueFromRequest to succeed, got error: %v", err)
	}
	if result != "string_value" {
		t.Errorf("Expected 'string_value', got '%s'", result)
	}

	// test missing key
	result, err = ExtractStringValueFromRequest(data, "missing_key")
	if err == nil {
		t.Errorf("Expected ExtractStringValueFromRequest to fail with missing key, got success")
	}

	// test non-string value
	data["str_key"] = 123
	result, err = ExtractStringValueFromRequest(data, "str_key")
	if err == nil {
		t.Errorf("Expected ExtractStringValueFromRequest to fail with non-string value, got success")
	}
}

func TestExtractIntFromRequest(t *testing.T) {
	// test successful extraction
	data := map[string]interface{}{
		"int_key": 42.0,
	}
	result, err := ExtractIntFromRequest(data, "int_key")
	if err != nil {
		t.Errorf("Expected ExtractIntFromRequest to succeed, got error: %v", err)
	}
	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}

	// test missing key
	result, err = ExtractIntFromRequest(data, "missing_key")
	if err == nil {
		t.Errorf("Expected ExtractIntFromRequest to fail with missing key, got success")
	}

	// test non-number value
	data["int_key"] = "not a number"
	result, err = ExtractIntFromRequest(data, "int_key")
	if err == nil {
		t.Errorf("Expected ExtractIntFromRequest to fail with non-number value, got success")
	}
}
