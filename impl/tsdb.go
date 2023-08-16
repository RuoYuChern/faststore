package impl

import (
	"container/list"
	"os"

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
	blkSize  uint32
	readOff  uint32
	dataType string
	impl     *fstTsdbImpl
	block    *Block
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
	offset   int
	low      int64
	high     int64
	impl     *fstTsdbImpl
	tRidx    *TsdbRangIndex
	tIdx     *TsdbIndex
	datCache *tsdbRDCache
}

type fstTsdbImpl struct {
	api.FastStoreCall
	table    string
	dataDir  string
	symbol   string
	appender *tsdbAppender
	query    *tsdbQuery
}

type fstLoggerImpl struct {
	api.FstLogger
	dir      string
	table    string
	ios      *os.File
	cache    []byte
	cacheOff uint32
}

type ftsdbEoff struct {
}

type ftsdbEmpty struct {
}

func NewTsdb(dir, table string, symbol string) *fstTsdbImpl {
	dataDir := dir
	return &fstTsdbImpl{table: table, dataDir: dataDir, symbol: symbol}
}

func NewLogger(dir, table string) *fstLoggerImpl {
	return &fstLoggerImpl{dir: dir, table: table}
}

func Tsdb_IsEoff(e error) bool {
	if e != nil && e == gErr_Eof {
		return true
	}
	return false
}

func Tsdb_IsEmpty(e error) bool {
	return ((e != nil) && (e == gErr_Empty))
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
		tsdb.query = &tsdbQuery{impl: tsdb}
	} else {
		tsdb.query.close()
	}
	return tsdb.query.getLastN(key, limit)
}
func (tsdb *fstTsdbImpl) GetBetween(low, high int64, offset int) (*list.List, error) {
	if tsdb.query == nil {
		tsdb.query = &tsdbQuery{low: low, high: high, offset: 0, impl: tsdb}
	} else {
		if (low != tsdb.query.low) || (high != tsdb.query.high) {
			tsdb.query.close()
			tsdb.query.high = high
			tsdb.query.low = low
		}
	}
	return tsdb.query.getBetween(low, high, offset)
}
func (tsdb *fstTsdbImpl) Close() {
	if tsdb.appender != nil {
		tsdb.appender.close()
	}
	if tsdb.query != nil {
		tsdb.query.close()
	}
}

func (e ftsdbEoff) Error() string {
	return "EOF"
}

func (e ftsdbEmpty) Error() string {
	return "EMPTY"
}
