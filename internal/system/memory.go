package system

import (
	"bufio"
	"errors"
	"math"
	"os"
	"strconv"
	"strings"
)

// GetTotalMemory returns the total available memory in bytes.
// It tries to read from cgroup limits first, then falls back to system memory.
func GetTotalMemory() (uint64, error) {
	// Try cgroup v2
	mem, err := getCgroupV2Memory()
	if err == nil {
		return mem, nil
	}

	// Try cgroup v1
	mem, err = getCgroupV1Memory()
	if err == nil {
		return mem, nil
	}

	// Fallback to system memory
	return getSystemMemory()
}

func getCgroupV2Memory() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0, errors.New("no limit set in cgroup v2")
	}
	return strconv.ParseUint(s, 10, 64)
}

func getCgroupV1Memory() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	mem, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	// A very large number indicates no limit in cgroup v1
	if mem > math.MaxInt64 {
		return 0, errors.New("no limit set in cgroup v1")
	}
	return mem, nil
}

func getSystemMemory() (uint64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			parts := strings.Fields(line)
			if len(parts) < 2 {
				return 0, errors.New("invalid MemTotal format")
			}
			kb, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return kb * 1024, nil
		}
	}
	return 0, errors.New("MemTotal not found in /proc/meminfo")
}
