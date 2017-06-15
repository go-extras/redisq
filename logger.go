package redisq

// Logger interface as implemented in https://github.com/sirupsen/logrus
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Printf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})
	Debug(args ...interface{})
	Info(args ...interface{})
	Print(args ...interface{})
	Warn(args ...interface{})
	Warning(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
	Panic(args ...interface{})
	Debugln(args ...interface{})
	Infoln(args ...interface{})
	Println(args ...interface{})
	Warnln(args ...interface{})
	Warningln(args ...interface{})
	Errorln(args ...interface{})
	Fatalln(args ...interface{})
	Panicln(args ...interface{})
}

// NullLogger class that does not log anything, but just implements the Logger interface
type NullLogger struct{}

func (logger *NullLogger) Debugf(format string, args ...interface{})   {}
func (logger *NullLogger) Infof(format string, args ...interface{})    {}
func (logger *NullLogger) Printf(format string, args ...interface{})   {}
func (logger *NullLogger) Warnf(format string, args ...interface{})    {}
func (logger *NullLogger) Warningf(format string, args ...interface{}) {}
func (logger *NullLogger) Errorf(format string, args ...interface{})   {}
func (logger *NullLogger) Fatalf(format string, args ...interface{})   {}
func (logger *NullLogger) Panicf(format string, args ...interface{})   {}
func (logger *NullLogger) Debug(args ...interface{})                   {}
func (logger *NullLogger) Info(args ...interface{})                    {}
func (logger *NullLogger) Print(args ...interface{})                   {}
func (logger *NullLogger) Warn(args ...interface{})                    {}
func (logger *NullLogger) Warning(args ...interface{})                 {}
func (logger *NullLogger) Error(args ...interface{})                   {}
func (logger *NullLogger) Fatal(args ...interface{})                   {}
func (logger *NullLogger) Panic(args ...interface{})                   {}
func (logger *NullLogger) Debugln(args ...interface{})                 {}
func (logger *NullLogger) Infoln(args ...interface{})                  {}
func (logger *NullLogger) Println(args ...interface{})                 {}
func (logger *NullLogger) Warnln(args ...interface{})                  {}
func (logger *NullLogger) Warningln(args ...interface{})               {}
func (logger *NullLogger) Errorln(args ...interface{})                 {}
func (logger *NullLogger) Fatalln(args ...interface{})                 {}
func (logger *NullLogger) Panicln(args ...interface{})                 {}
