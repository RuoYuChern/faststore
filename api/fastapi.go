package api

import "container/list"

type FstTsdbValue struct {
	Timestamp int64
	Data      []byte
}

type TsdbConf struct {
	Level      string `yaml:"level"`
	File       string `yaml:"log_file"`
	MaxSize    int    `yaml:"max_size"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`
	Env        string `yaml:"env"`
	DataDir    string `yaml:"data"`
}

type FstTsdbCall interface {
	Symbol() string
	Append(value *FstTsdbValue) error
	GetLastN(key int64, limit int) (*list.List, error)
	GetBetween(low, high int64, limit int) (*list.List, error)
	Close()
}

type FastStoreCall interface {
	Key() string
	Save(data []byte) error
	Append(data []byte) error
	Close()
}
