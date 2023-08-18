package api

import "container/list"

var DEF_LIMIT = 1000

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
	GetBetween(low, high int64, off int) (*list.List, error)
	Close()
}

type FstLogger interface {
	Append(key string, value *FstTsdbValue) error
	ForEach(call func(key string, value *FstTsdbValue) error) error
	Close()
}

type FastStoreCall interface {
	Key() string
	Save(data []byte) error
	Append(data []byte) error
	Read(data []byte) (int, error)
	Close()
}
