package logger

type NullLogger struct{}

func (n *NullLogger) Debugf(string, ...interface{}) {}
func (n *NullLogger) Infof(string, ...interface{})  {}
func (n *NullLogger) Errorf(string, ...interface{}) {}
func (n *NullLogger) SetLevel(string) error         { return nil }
