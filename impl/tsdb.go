package impl

import (
	"container/list"
	"fmt"

	"github.com/tao/faststore/api"
)

type tsdbWRCache struct {
	blkSize   uint32
	cacheType int
	dataType  string
	impl      *fstTsdbImpl
	addr      *BlockAddr
	block     *Block
}

type tsdbRDCache struct {
	blkSize   uint32
	readOff   uint32
	cacheType int
	dataType  string
	impl      *fstTsdbImpl
	block     *Block
}

type tsdbAppender struct {
	topRef    *BlockAddr
	impl      *fstTsdbImpl
	lastRidx  *TsdbRangIndex
	ridxCache *tsdbWRCache
	idxCache  *tsdbWRCache
	datCache  *tsdbWRCache
}

type tsdbQuery struct {
	forward   bool
	offset    int
	impl      *fstTsdbImpl
	ridxCache *tsdbRDCache
	idxCache  *tsdbRDCache
	datCache  *tsdbRDCache
}

type fstTsdbImpl struct {
	api.FastStoreCall
	table    string
	dataDir  string
	symbol   string
	appender *tsdbAppender
	query    *tsdbQuery
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
	if tsdb.query == nil {
		tsdb.query = &tsdbQuery{forward: true, impl: tsdb}
	}
	return tsdb.query.getLastN(key, limit)
}
func (tsdb *fstTsdbImpl) GetBetween(low, high int64, offset int) (*list.List, error) {
	if tsdb.query == nil {
		tsdb.query = &tsdbQuery{forward: false, offset: 0, impl: tsdb}
	}
	return tsdb.query.getBetween(low, high, offset)
}
func (tsdb *fstTsdbImpl) Close() {
	if tsdb.appender != nil {
		tsdb.appender.close()
	}
}
