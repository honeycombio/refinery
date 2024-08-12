package sharder

type MockSharder struct {
	Self  *TestShard
	Other *TestShard
}

func (s *MockSharder) MyShard() Shard { return s.Self }

func (s *MockSharder) WhichShard(traceID string) Shard {
	if s.Other != nil {
		return s.Other
	}

	return s.Self
}

type TestShard struct {
	Addr string
}

func (s *TestShard) Equals(other Shard) bool { return s.Addr == other.GetAddress() }
func (s *TestShard) GetAddress() string      { return s.Addr }
