package config

import (
	"fmt"
	"strconv"
	"strings"
)

// parseVersion parses a version string like "v2.9.7" or "2.9.7" into major, minor, patch integers
func parseVersion(version string) (major, minor, patch int, err error) {
	// Remove 'v' prefix if present
	version = strings.TrimPrefix(version, "v")

	parts := strings.Split(version, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, 0, 0, fmt.Errorf("invalid version format: %s", version)
	}

	major, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	patch = 0
	if len(parts) == 3 {
		patch, err = strconv.Atoi(parts[2])
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid patch version: %s", parts[2])
		}
	}

	return major, minor, patch, nil
}

// compareVersions compares two version strings
// Returns:
//
//	-1 if current < target
//	 0 if current == target
//	 1 if current > target
//	error if versions are malformed
func compareVersions(current, target string) (int, error) {
	if current == "dev" {
		return 1, nil
	}

	currentMajor, currentMinor, currentPatch, err := parseVersion(current)
	if err != nil {
		return 0, fmt.Errorf("failed to parse current version %s: %w", current, err)
	}

	targetMajor, targetMinor, targetPatch, err := parseVersion(target)
	if err != nil {
		return 0, fmt.Errorf("failed to parse target version %s: %w", target, err)
	}

	if currentMajor != targetMajor {
		if currentMajor < targetMajor {
			return -1, nil
		}
		return 1, nil
	}

	if currentMinor != targetMinor {
		if currentMinor < targetMinor {
			return -1, nil
		}
		return 1, nil
	}

	if currentPatch != targetPatch {
		if currentPatch < targetPatch {
			return -1, nil
		}
		return 1, nil
	}

	return 0, nil
}

// isVersionBefore returns true if current version is before the target version
func isVersionBefore(current, target string) bool {
	fmt.Println("current", current)
	result, err := compareVersions(current, target)
	if err != nil {
		// If we can't parse versions, assume we should show the warning
		return true
	}
	return result < 0
}
