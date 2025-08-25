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
	if !semver.IsValid(current) {
		return 0, fmt.Errorf("invalid version format: %s", current)
	}

	if !semver.IsValid(target) {
		return 0, fmt.Errorf("invalid version format: %s", target)
	}

	return semver.Compare(current, target), nil
}

// isVersionBefore returns true if current version is before the target version
func isVersionBefore(current, target string) bool {
	result, err := compareVersions(current, target)
	if err != nil {
		// If we can't parse versions, assume we should show the warning
		return true
	}
	return result < 0
}
