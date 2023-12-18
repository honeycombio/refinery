package config

import "testing"

func TestMemorySize_UnmarshalText(t *testing.T) {
	tests := []struct {
		text    string
		value   uint64
		wantErr bool
	}{
		{"1", 1, false},
		{"1K", 1 * K, false},
		{"1M", 1 * M, false},
		{"1G", 1 * G, false},
		{"1T", 1 * T, false},
		{"1P", 1 * P, false},
		{"1E", 1 * E, false},
		{"1Ki", 1 * Ki, false},
		{"1Mi", 1 * Mi, false},
		{"1Gi", 1 * Gi, false},
		{"1Ti", 1 * Ti, false},
		{"1Pi", 1 * Pi, false},
		{"1Ei", 1 * Ei, false},
		{"1.5K", 1*K + 500, false},
		{"1.5M", 1*M + 500*K, false},
		{"1.5G", 1*G + 500*M, false},
		{"1.5T", 1*T + 500*G, false},
		{"1.5P", 1*P + 500*T, false},
		{"1.5E", 1*E + 500*P, false},
		{"1.5Ki", 1*Ki + 512, false},
		{"1.5Mi", 1*Mi + 512*Ki, false},
		{"1.5Gi", 1*Gi + 512*Mi, false},
		{"1.5Ti", 1*Ti + 512*Gi, false},
		{"1.5Pi", 1*Pi + 512*Ti, false},
		{"1.5Ei", 1*Ei + 512*Pi, false},
		{"1.5", 1, false},
		{"1.5.1", 0, true},
		{"-1K", 0, true},
		{"  4.5G", 4*G + 500*M, false},
		{"4.5G  ", 4*G + 500*M, false},
		{"1717600000 ", 1*G + 717*M + 600*K, false},
		{" 1717600000 ", 1*G + 717*M + 600*K, false},
		{" 1717600000", 1*G + 717*M + 600*K, false},
		{"1717600K", 1*G + 717*M + 600*K, false},
		{"100_000_000", 100 * M, false},
		{"1G", 1 * G, false},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			var ms MemorySize
			if err := ms.UnmarshalText([]byte(tt.text)); (err != nil) != tt.wantErr {
				t.Errorf("MemorySize.UnmarshalText() error = %v, wantErr %v", err, tt.wantErr)
			}
			if uint64(ms) != tt.value {
				t.Errorf("MemorySize.UnmarshalText() = %v, want %v", ms, tt.value)
			}
		})
	}
}

func TestMemorySize_Roundtrip(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{"1", "1"},
		{"1K", "1K"},
		{"1M", "1M"},
		{"1G", "1G"},
		{"1T", "1T"},
		{"1P", "1P"},
		{"1E", "1E"},
		{"1Ki", "1Ki"},
		{"1Mi", "1Mi"},
		{"1Gi", "1Gi"},
		{"1Ti", "1Ti"},
		{"1Pi", "1Pi"},
		{"1Ei", "1Ei"},
		{"1.5K", "1500"},
		{"1.5M", "1500K"},
		{"1.5G", "1500M"},
		{"1.5T", "1500G"},
		{"1.5P", "1500T"},
		{"1.5E", "1500P"},
		{"1.5Ki", "1536"},
		{"1.5Mi", "1536Ki"},
		{"1.5Gi", "1536Mi"},
		{"1.5Ti", "1536Gi"},
		{"1.5Pi", "1536Ti"},
		{"1.5Ei", "1536Pi"},
		{"55834574848", "52Gi"},
		{"55834574847", "55834574847"},
		{"1717600000", "1717600K"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var ms MemorySize
			if err := ms.UnmarshalText([]byte(tt.input)); err != nil {
				t.Errorf("MemorySize.MarshalText() error = %v", err)
			}
			b, err := ms.MarshalText()
			if err != nil {
				t.Errorf("MemorySize.MarshalText() error = %v", err)
			}
			if string(b) != tt.output {
				t.Errorf("MemorySize.MarshalText() = %v, want %v", string(b), tt.output)
			}
		})
	}
}
