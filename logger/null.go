package logger

type NullLogger struct{}

func (n *NullLogger) WithField(key string, value interface{}) Entry  { return &NullLoggerEntry{} }
func (n *NullLogger) WithFields(fields map[string]interface{}) Entry { return &NullLoggerEntry{} }
func (n *NullLogger) Debugf(string, ...interface{})                  {}
func (n *NullLogger) Infof(string, ...interface{})                   {}
func (n *NullLogger) Errorf(string, ...interface{})                  {}
func (n *NullLogger) SetLevel(string) error                          { return nil }

type NullLoggerEntry struct{}

func (n *NullLoggerEntry) WithField(key string, value interface{}) Entry  { return &NullLoggerEntry{} }
func (n *NullLoggerEntry) WithFields(fields map[string]interface{}) Entry { return &NullLoggerEntry{} }
func (n *NullLoggerEntry) Debugf(string, ...interface{})                  {}
func (n *NullLoggerEntry) Infof(string, ...interface{})                   {}
func (n *NullLoggerEntry) Errorf(string, ...interface{})                  {}
