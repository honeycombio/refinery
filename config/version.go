package config

import (
	"fmt"

	"golang.org/x/mod/semver"
)

// compareVersions compares two version strings using semantic versioning
// Returns:
//
//	-1 if current < target
//	 0 if current == target
//	 1 if current > target
//	error if versions are malformed
func compareVersions(current, target string) (int, error) {
	currentValue := current
	targetValue := target
	if !semver.IsValid(currentValue) {
		currentValue = "v" + currentValue
		if !semver.IsValid(currentValue) {
			return 0, fmt.Errorf("invalid version format: %s", current)
		}
	}

	if !semver.IsValid(targetValue) {
		targetValue = "v" + targetValue
		if !semver.IsValid(targetValue) {
			return 0, fmt.Errorf("invalid version format: %s", target)
		}
	}

	return semver.Compare(currentValue, targetValue), nil
}

// isVersionDeprecated returns true if the current version is equal or before the target version.
func isVersionDeprecated(current, target string) bool {
	result, err := compareVersions(current, target)
	if err != nil {
		// If we can't parse versions, assume we should show the warning
		return true
	}
	return result < 1
}
