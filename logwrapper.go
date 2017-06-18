package redisq

import "gopkg.in/go-extras/redisq.v1"

type LogWrapper struct {
	logger redisq.Logger
	prefix string
}

func WrapLogger(logger redisq.Logger, prefix string) *LogWrapper {
	return &LogWrapper{
		logger: logger,
		prefix: prefix,
	}
}

func (logger *LogWrapper) Debugf(format string, args ...interface{}) {
	logger.logger.Debugf(logger.prefix+" "+format, args...)
}
func (logger *LogWrapper) Infof(format string, args ...interface{}) {
	logger.logger.Infof(logger.prefix+" "+format, args...)
}
func (logger *LogWrapper) Printf(format string, args ...interface{}) {
	logger.logger.Printf(logger.prefix+" "+format, args...)
}
func (logger *LogWrapper) Warnf(format string, args ...interface{}) {
	logger.logger.Warnf(logger.prefix+" "+format, args...)
}
func (logger *LogWrapper) Warningf(format string, args ...interface{}) {
	logger.logger.Warningf(logger.prefix+" "+format, args...)
}
func (logger *LogWrapper) Errorf(format string, args ...interface{}) {
	logger.logger.Errorf(logger.prefix+" "+format, args...)
}
func (logger *LogWrapper) Fatalf(format string, args ...interface{}) {
	logger.logger.Fatalf(logger.prefix+" "+format, args...)
}
func (logger *LogWrapper) Panicf(format string, args ...interface{}) {
	logger.logger.Panicf(logger.prefix+" "+format, args...)
}
func (logger *LogWrapper) Debug(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Debug(args...)
}
func (logger *LogWrapper) Info(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Info(args...)
}
func (logger *LogWrapper) Print(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Print(args...)
}
func (logger *LogWrapper) Warn(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Warn(args...)
}
func (logger *LogWrapper) Warning(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Warning(args...)
}
func (logger *LogWrapper) Error(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Error(args...)
}
func (logger *LogWrapper) Fatal(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Fatal(args...)
}
func (logger *LogWrapper) Panic(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Panic(args...)
}
func (logger *LogWrapper) Debugln(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Debugln(args...)
}
func (logger *LogWrapper) Infoln(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Infoln(args...)
}
func (logger *LogWrapper) Println(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Println(args...)
}
func (logger *LogWrapper) Warnln(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Warnln(args...)
}
func (logger *LogWrapper) Warningln(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Warningln(args...)
}
func (logger *LogWrapper) Errorln(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Errorln(args...)
}
func (logger *LogWrapper) Fatalln(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Fatalln(args...)
}
func (logger *LogWrapper) Panicln(args ...interface{}) {
	args = append([]interface{}{logger.prefix}, args...)
	logger.logger.Panicln(args...)
}
