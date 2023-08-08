package impl

import (
	"container/list"
	"fmt"

	"github.com/tao/faststore/api"
)

type tsdbAppender struct {
	topRef   *BlockRefer
	tailRef  *BlockRefer
	impl     *fstTsdbImpl
	lastRidx *TsdbRangIndex
}

type fstTsdbImpl struct {
	api.FastStoreCall
	table    string
	dataDir  string
	symbol   string
	appender *tsdbAppender
}

func NewTsdb(dir, table string, symbol string) *fstTsdbImpl {
	dataDir := fmt.Sprintf("%s/%s", dir, table)
	return &fstTsdbImpl{table: table, dataDir: dataDir, symbol: symbol}
}

func (tsdb *fstTsdbImpl) Symbol() string {
	return tsdb.symbol
}
func (tsdb *fstTsdbImpl) Append(value *api.FstTsdbValue) error {
	if tsdb.appender == nil {
		tsdb.appender = &tsdbAppender{impl: tsdb}
	}
	return tsdb.appender.append(value)
}
func (tsdb *fstTsdbImpl) GetLastN(key int64, limit int) (*list.List, error) {
	return nil, nil
}
func (tsdb *fstTsdbImpl) GetBetween(low, high int64, limit int) (*list.List, error) {
	return nil, nil
}
func (tsdb *fstTsdbImpl) Close() {

}
