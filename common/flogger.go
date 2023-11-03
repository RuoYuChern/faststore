package common

import (
	"log"
	"os"

	"github.com/tao/faststore/api"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Logger *zap.SugaredLogger

func InitLogger(c *api.TsdbConf) {
	syncer := initLogWriter(c)
	encoder := initEncoder()
	level, err := zapcore.ParseLevel(c.Level)
	if err != nil {
		log.Fatalf("ParseLevel:%s failed", c.Level)
		level = zapcore.InfoLevel
	}

	highPriority := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return (l >= level)
	})

	console := zapcore.Lock(os.Stdout)
	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	var core zapcore.Core
	if c.Env == "dev" {
		core = zapcore.NewTee(zapcore.NewCore(encoder, syncer, highPriority), zapcore.NewCore(consoleEncoder, console, highPriority))
	} else {
		core = zapcore.NewCore(encoder, syncer, highPriority)
	}

	//开启文件及行号
	development := zap.Development()
	Logger = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel), development).Sugar()
}

func initLogWriter(c *api.TsdbConf) zapcore.WriteSyncer {
	logger := &lumberjack.Logger{
		Filename:   c.File,
		MaxSize:    c.MaxSize,
		MaxBackups: c.MaxBackups,
		MaxAge:     c.MaxAge,
		Compress:   false,
	}
	return zapcore.AddSync(logger)
}

func initEncoder() zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(cfg)
}
