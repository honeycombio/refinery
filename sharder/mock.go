package sharder

type TestSharder struct{}

func (s *TestSharder) MyShard() Shard { return nil }

func (s *TestSharder) WhichShard(string) Shard {
	return &TestShard{
		addr: "http://localhost:12345",
	}
}

type TestShard struct {
	addr string
}

func (s *TestShard) Equals(other Shard) bool { return true }
func (s *TestShard) GetAddress() string      { return s.addr }
