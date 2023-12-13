package config

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/jessevdk/go-flags"
)

const (
	K = uint64(1000)
	M = 1000 * K
	G = 1000 * M
	T = 1000 * G
	P = 1000 * T
	E = 1000 * P

	Ki = uint64(1024)
	Mi = 1024 * Ki
	Gi = 1024 * Mi
	Ti = 1024 * Gi
	Pi = 1024 * Ti
	Ei = 1024 * Pi

	invalidSizeError = "invalid size: %s"
)

var unitSlice = []uint64{
	Ei,
	E,
	Pi,
	P,
	Ti,
	T,
	Gi,
	G,
	Mi,
	M,
	Ki,
	K,
	1,
}

var reverseUnitMap = map[uint64]string{
	Ki: "Ki",
	Mi: "Mi",
	Gi: "Gi",
	Ti: "Ti",
	Pi: "Pi",
	Ei: "Ei",
	K:  "K",
	M:  "M",
	G:  "G",
	T:  "T",
	P:  "P",
	E:  "E",
}

var unitMap = map[string]uint64{
	"bi":  1,
	"bib": 1,
	"ki":  Ki,
	"kib": Ki,
	"mi":  Mi,
	"mib": Mi,
	"gi":  Gi,
	"gib": Gi,
	"ti":  Ti,
	"tib": Ti,
	"pi":  Pi,
	"pib": Pi,
	"ei":  Ei,
	"eib": Ei,
	"b":   1,
	"bb":  1,
	"k":   K,
	"kb":  K,
	"m":   M,
	"mb":  M,
	"g":   G,
	"gb":  G,
	"t":   T,
	"tb":  T,
	"p":   P,
	"pb":  P,
	"e":   E,
	"eb":  E,
}

// We also use a special type for memory sizes
type MemorySize uint64

func (m MemorySize) MarshalText() ([]byte, error) {
	if m > 0 {
		for _, size := range unitSlice {
			result := float64(m) / float64(size)
			if result == math.Trunc(result) {
				unit, ok := reverseUnitMap[size]
				if !ok {
					break
				}
				return []byte(fmt.Sprintf("%.0f%v", result, unit)), nil
			}
		}
	}
	return []byte(fmt.Sprintf("%v", m)), nil
}

// We accept floating point specifications but convert them ultimately to a uint64.
func (m *MemorySize) UnmarshalText(text []byte) error {
	txt := string(text)

	r := regexp.MustCompile(`^\s*(?P<number>[0-9\._]+)(?P<unit>[a-zA-Z]*)\s*$`)
	matches := r.FindStringSubmatch(strings.ToLower(txt))
	if matches == nil {
		return fmt.Errorf(invalidSizeError, txt)
	}

	var number float64
	var unit string
	var err error
	for i, name := range r.SubexpNames() {
		if i == 0 {
			continue
		}

		switch name {
		case "number":
			number, err = strconv.ParseFloat(matches[i], 64)
			if err != nil {
				return fmt.Errorf(invalidSizeError, text)
			}
		case "unit":
			unit = matches[i]
		default:
			return fmt.Errorf("error converting %v to MemorySize, this is a bug with Refinery", text)
		}
	}

	// No unit means bytes
	if unit == "" {
		*m = MemorySize(number)
	} else {
		scalar, ok := unitMap[unit]
		if !ok {
			return fmt.Errorf(invalidSizeError, text)
		}
		*m = MemorySize(number * float64(scalar))
	}
	return nil
}

// Make sure we implement flags.Unmarshaler so that it works with cmdenv.
var _ flags.Unmarshaler = (*MemorySize)(nil)

func (m *MemorySize) UnmarshalFlag(value string) error {
	return m.UnmarshalText([]byte(value))
}
