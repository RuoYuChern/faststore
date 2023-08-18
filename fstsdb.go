package faststore

import (
	"github.com/tao/faststore/api"
	"github.com/tao/faststore/common"
	"github.com/tao/faststore/impl"
)

var conf *api.TsdbConf

func Start(c *api.TsdbConf) error {
	conf = c
	common.InitLogger(c)
	if err := impl.StartDb(c); err != nil {
		return err
	}
	return nil
}

func Stop() {
	impl.StopDb()
}

func FsTsdbGet(table, key string) api.FstTsdbCall {
	return impl.NewTsdb(conf.DataDir, table, key)
}

func FsTsdbLogGet(table string) api.FstLogger {
	return impl.NewLogger(conf.DataDir, table)
}
