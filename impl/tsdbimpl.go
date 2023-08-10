package impl

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/tao/faststore/api"
	"github.com/tao/faststore/common"
)

var gAlocLock sync.Mutex
var gRidxPool = sync.Pool{
	New: func() any {
		buf := make([]byte, gBLK_RIDX_SIZE)
		return buf
	},
}

var gIdxPool = sync.Pool{
	New: func() any {
		buf := make([]byte, gBLK_IDX_SIZE)
		return buf
	},
}

var gObjPool = sync.Pool{
	New: func() any {
		buf := make([]byte, gBLK_OBJ_SIZE)
		return buf
	},
}

var (
	gData_RIDX  = "ridx"
	gData_IDX   = "idx"
	gData_VAL   = "leaf"
	gCache_RIDX = 1
	gCache_IDX  = 2
	gCache_VAL  = 3
)

func (ta *tsdbAppender) append(value *api.FstTsdbValue) error {
	err := ta.getTailRIdx()
	if err != nil {
		common.Logger.Infof("value=%d, failed:%s", value.Timestamp, err)
		return err
	}
	// 只支持追加写
	if ta.lastRidx != nil && value.Timestamp < int64(ta.lastRidx.High) {
		return nil
	}
	return ta.appendData(value)
}

func (ta *tsdbAppender) close() {
	if ta.lastRidx != nil {
		err := ta.ridxCache.updateTail(ta.lastRidx)
		if err != nil {
			common.Logger.Infof("updateTail failed:%s", err)
			return
		}
	}
	if ta.datCache != nil {
		err := ta.datCache.close()
		if err != nil {
			common.Logger.Infof("datCache close failed:%s", err)
			return
		}
	}
	if ta.idxCache != nil {
		err := ta.idxCache.close()
		if err != nil {
			common.Logger.Infof("idxCache close failed:%s", err)
			return
		}
	}
	if ta.ridxCache != nil {
		err := ta.ridxCache.close()
		if err != nil {
			common.Logger.Infof("ridxCache close failed:%s", err)
			return
		}
	}
	if ta.topRef != nil {
		err := saveTsData(ta.impl.table, ta.impl.symbol, ta.topRef)
		if err != nil {
			common.Logger.Infof("saveTsData failed:%s", err)
			return
		}
	}
}

func (ta *tsdbAppender) appendData(value *api.FstTsdbValue) error {
	err := ta.getDataCache()
	if err != nil {
		return err
	}

	//append data
	tsValue := &TsdbValue{Timestamp: value.Timestamp, Data: value.Data}
	addr, err := ta.datCache.append(tsValue)
	if err != nil {
		return err
	}
	// append idx
	tidx := &TsdbIndex{Key: uint64(value.Timestamp), Addr: BlockAddr{SegNo: addr.SegNo, SegOffset: addr.SegOffset}}
	addr, err = ta.idxCache.append(tidx)
	if err != nil {
		return err
	}
	if ta.lastRidx == nil {
		segOff := (addr.SegOffset / gBLK_RIDX_SIZE) * gBLK_RIDX_SIZE
		tRidx := &TsdbRangIndex{Low: uint64(value.Timestamp), High: uint64(value.Timestamp + 1), Off: 0, Addr: BlockAddr{SegNo: addr.SegNo, SegOffset: segOff}}
		ta.lastRidx = tRidx
	} else {
		if ta.lastRidx.Addr.SegNo == addr.SegNo {
			/**没有换新的idxBlock**/
			ta.lastRidx.High = uint64(value.Timestamp + 1)
			return nil
		}
		err = ta.ridxCache.updateTail(ta.lastRidx)
		if err != nil {
			return err
		}
		segOff := (addr.SegOffset / gBLK_RIDX_SIZE) * gBLK_RIDX_SIZE
		tRidx := &TsdbRangIndex{Low: uint64(value.Timestamp), High: uint64(value.Timestamp + 1), Off: 0, Addr: BlockAddr{SegNo: addr.SegNo, SegOffset: segOff}}
		ta.lastRidx = tRidx
	}
	return nil
}

func (ta *tsdbAppender) getDataCache() error {
	if ta.datCache != nil {
		return nil
	}
	if ta.lastRidx == nil {
		//分配idx cache
		cache, err := allocBlockByType(gData_IDX, nil, ta.impl)
		if err != nil {
			return err
		}
		ta.idxCache = cache
		cache, err = allocBlockByType(gData_VAL, nil, ta.impl)
		if err != nil {
			return err
		}
		ta.datCache = cache
		return nil
	}
	// 查找位置
	blk := &Block{}
	err := loadBlock(&ta.lastRidx.Addr, ta.impl.dataDir, ta.impl.table, gData_IDX, blk)
	if err != nil {
		return err
	}
	idxAdr := &BlockAddr{SegNo: ta.lastRidx.Addr.SegNo, SegOffset: ta.lastRidx.Addr.SegOffset}
	ta.idxCache = &tsdbWRCache{size: getTypeSize(gData_IDX), impl: ta.impl, addr: idxAdr, block: blk}
	idxItem := &TsdbIndex{}
	err = idxItem.UnmarshalBinary(blk.Data[(blk.BH.Len - uint32(gTSDB_IDX_LEN)):])
	if err != nil {
		return err
	}
	datAddr := &BlockAddr{SegNo: idxItem.Addr.SegNo, SegOffset: (idxItem.Addr.SegOffset / uint32(gBLK_OBJ_SIZE)) * uint32(gBLK_OBJ_SIZE)}
	datBlk := &Block{}
	err = loadBlock(datAddr, ta.impl.dataDir, ta.impl.table, gData_VAL, datBlk)
	if err != nil {
		return err
	}
	ta.datCache = &tsdbWRCache{size: getTypeSize(gData_VAL), impl: ta.impl, addr: datAddr, block: datBlk}
	return nil
}

func (ta *tsdbAppender) getTailRIdx() error {
	if ta.lastRidx != nil {
		return nil
	}
	isNew := false
	if ta.topRef == nil {
		ta.topRef = &BlockAddr{}
		err := getTsData(ta.impl.table, ta.impl.symbol, ta.topRef)
		if err != nil {
			ref, err := alloc(ta.impl.dataDir, ta.impl.table, gData_RIDX)
			if err != nil {
				return err
			}
			ta.topRef.SegNo = ref.SegNo
			ta.topRef.SegOffset = ref.SegOffset
			isNew = true
		}
	}

	if isNew {
		//初始化
		ta.ridxCache = newBlockCache(gData_RIDX, nil, &BlockAddr{SegNo: ta.topRef.SegNo, SegOffset: ta.topRef.SegOffset}, ta.impl)
	} else {
		block := &Block{}
		addr := &BlockAddr{SegNo: ta.topRef.SegNo, SegOffset: ta.topRef.SegOffset}
		for {
			err := loadBlock(addr, ta.impl.dataDir, ta.impl.table, gData_RIDX, block)
			if err != nil {
				common.Logger.Infof("getBlock failed:%s", err)
				return err
			}
			if block.BH.Next.SegNo != 0 {
				addr.SegNo = block.BH.Next.SegNo
				addr.SegOffset = block.BH.Next.SegOffset
				continue
			}
			buf := make([]byte, gTSDB_RIDX_LEN)
			bh := &block.BH
			tailAdr := &BlockAddr{SegNo: addr.SegNo, SegOffset: addr.SegOffset}
			ta.ridxCache = &tsdbWRCache{size: gBLK_RIDX_SIZE, impl: ta.impl, addr: tailAdr, block: block}
			bcopy(buf, block.Data, 0, bh.Len-gTSDB_RIDX_LEN, gTSDB_RIDX_LEN)
			ridx := &TsdbRangIndex{}
			err = ridx.UnmarshalBinary(buf)
			if err != nil {
				common.Logger.Infof("UnmarshalBinary failed:%s", err)
				return err
			}
			ta.lastRidx = ridx
		}
	}
	return nil
}

func (ca *tsdbWRCache) updateTail(data *TsdbRangIndex) error {
	totalLen := gTSDB_RIDX_LEN + ca.block.BH.Len + gBH_LEN
	if data.Off == 0 {
		data.Off = (ca.block.BH.Len / gTSDB_RIDX_LEN) + 1
		if totalLen > uint32(ca.size) {
			data.Off = 1
			err := ca.changeCache()
			if err != nil {
				return err
			}
		}
	}
	out, err := data.MarshalBinary()
	if err != nil {
		return err
	}
	off := (data.Off - 1) * gTSDB_RIDX_LEN
	if off >= ca.block.BH.Len {
		_ = ca.toCache(gTSDB_RIDX_LEN, out)
	} else {
		bcopy(ca.block.Data, out, off, 0, gTSDB_RIDX_LEN)
	}
	return nil
}

func (ca *tsdbWRCache) append(data FsData) (*BlockAddr, error) {
	out, err := data.MarshalBinary()
	if err != nil {
		return nil, err
	}
	dLen := uint32(len(out))
	outLen := dLen
	if ca.cacheType == gCache_VAL {
		outLen += gBLK_V_H_LEN
	}

	newLen := outLen + gBH_LEN
	if newLen >= ca.size {
		return nil, errors.New("too long data")
	}

	if (ca.block.BH.Len + newLen) <= ca.size {
		addr := ca.toCache(dLen, out)
		return addr, nil
	}

	err = ca.changeCache()
	if err != nil {
		return nil, err
	}

	if (ca.block.BH.Len + newLen) <= ca.size {
		addr := ca.toCache(dLen, out)
		return addr, nil
	}
	return nil, errors.New("len error")
}

func (ca *tsdbWRCache) changeCache() error {
	newCache, err := allocBlockByType(ca.dataType, ca.addr, ca.impl)
	if err != nil {
		return err
	}
	ca.block.BH.Next.SegNo = newCache.addr.SegNo
	ca.block.BH.Next.SegOffset = newCache.addr.SegOffset
	err = saveBlock(ca.addr, ca.impl.dataDir, ca.impl.table, ca.dataType, ca.block)
	if err != nil {
		return err
	}
	ca.block = newCache.block
	ca.addr = newCache.addr
	return nil
}

func (ca *tsdbWRCache) toCache(dLen uint32, out []byte) *BlockAddr {
	if ca.cacheType == gCache_VAL {
		PutIntToB(ca.block.Data[ca.block.BH.Len:], dLen)
		bcopy(ca.block.Data, out, ca.block.BH.Len+4, 0, dLen)
	} else {
		bcopy(ca.block.Data, out, ca.block.BH.Len, 0, dLen)
	}
	/**要加上头部长度**/
	segOff := ca.addr.SegOffset + (ca.block.BH.Len + gBH_LEN)
	addr := &BlockAddr{SegNo: ca.addr.SegNo, SegOffset: segOff}
	ca.block.BH.Len += dLen
	return addr
}

func (ca *tsdbWRCache) close() error {
	err := saveBlock(ca.addr, ca.impl.dataDir, ca.impl.table, ca.dataType, ca.block)
	if err != nil {
		return err
	}
	return nil
}

func allocBlockByType(datype string, pre *BlockAddr, impl *fstTsdbImpl) (*tsdbWRCache, error) {
	ref, err := alloc(impl.dataDir, impl.table, datype)
	if err != nil {
		return nil, err
	}
	return newBlockCache(datype, pre, ref, impl), nil
}

func newBlockCache(datype string, pre, ref *BlockAddr, impl *fstTsdbImpl) *tsdbWRCache {
	dataSize := getTypeSize(datype)
	cache := &tsdbWRCache{size: dataSize, dataType: datype, cacheType: getCacheType(datype), impl: impl}
	blk := &Block{BH: BlockHeader{}, Data: make([]byte, (dataSize - gBH_LEN))}
	cache.block = blk
	cache.addr = ref
	if pre != nil {
		blk.BH.Pre.SegNo = ref.SegNo
		blk.BH.Pre.SegOffset = ref.SegOffset
	} else {
		blk.BH.Pre.SegNo = 0
		blk.BH.Pre.SegOffset = 0
	}
	blk.BH.Next.SegNo = 0
	blk.BH.Next.SegOffset = 0
	return cache
}

func alloc(dir, table, datype string) (*BlockAddr, error) {
	gAlocLock.Lock()
	ba := &BlockAloc{SegNo: 0, AlocLen: 0}
	newBa := &BlockAddr{}
	err := getTsData(table, fmt.Sprintf("tsdb.%s.spb", datype), ba)
	datSize := getTypeSize(datype)
	if err == nil {
		if (ba.AlocLen + datSize) <= gBLK_FILE_SZ {
			newBa.SegNo = ba.SegNo
			newBa.SegOffset = ba.AlocLen
			//update bLen
			ba.AlocLen += uint32(datSize)
			err = saveTsData(table, fmt.Sprintf("tsdb.%s.spb", datype), ba)
			gAlocLock.Unlock()
			if err != nil {
				return nil, err
			}
			return newBa, nil
		} else {
			//需要重新分配(segment)
			ba.SegNo = ba.SegNo + 1
			ba.AlocLen = uint32(datSize)
			newBa.SegNo = ba.SegNo
			newBa.SegOffset = 0
		}
	} else {
		//第一块(segment)
		ba.SegNo = uint32(1)
		newBa.SegNo = ba.SegNo
		newBa.SegOffset = 0
		ba.AlocLen = uint32(datSize)
	}
	err = newSegment(ba.SegNo, dir, table, datype)
	if err != nil {
		gAlocLock.Unlock()
		return nil, err
	}
	err = saveTsData(table, fmt.Sprintf("tsdb.%s.spb", datype), ba)
	gAlocLock.Unlock()
	if err != nil {
		return nil, err
	}
	return newBa, nil
}

func newSegment(blockNo uint32, dir, table, datype string) error {
	os.MkdirAll(fmt.Sprintf("%s/ftsdb/%s", dir, table), 0755)
	name := fmt.Sprintf("%s/ftsdb/%s/%d_.%s", dir, table, blockNo, datype)
	fout, err := os.OpenFile(name, os.O_CREATE, 0755)
	if err != nil {
		common.Logger.Infof("newSegment name=%s open failed:%s", name, err)
		return err
	}
	err = fout.Truncate(int64(gBLK_FILE_SZ))
	fout.Close()
	if err != nil {
		common.Logger.Infof("newSegment name=%s truncate failed:%s", name, err)
		return err
	}
	return nil
}

func loadBlock(blk *BlockAddr, dir, table, datype string, data FsData) error {
	name := fmt.Sprintf("%s/ftsdb/%s/%d_.%s", dir, table, blk.SegNo, datype)
	fout, err := os.OpenFile(name, os.O_RDONLY, 0755)
	if err != nil {
		common.Logger.Infof("getBlock name=%s open failed:%s", name, err)
		return err
	}
	buf := getBlockBuffer(datype)
	_, err = fout.ReadAt(buf, int64(blk.SegOffset))
	fout.Close()
	if err != nil {
		common.Logger.Infof("ReadAt name=%s open failed:%s", name, err)
		putBlockBuff(datype, buf)
		return err
	}
	err = data.UnmarshalBinary(buf)
	putBlockBuff(datype, buf)
	if err != nil {
		common.Logger.Infof("UnmarshalBinary name=%s open failed:%s", name, err)
		return err
	}
	return nil
}

func saveBlock(blk *BlockAddr, dir, table, datype string, data FsData) error {
	name := fmt.Sprintf("%s/ftsdb/%s/%d_.%s", dir, table, blk.SegNo, datype)
	fout, err := os.OpenFile(name, os.O_WRONLY, 0755)
	if err != nil {
		common.Logger.Infof("saveBlock name=%s open failed:%s", name, err)
		return err
	}
	buf, err := data.MarshalBinary()
	if err != nil {
		common.Logger.Infof("saveBlock name=%s MarshalBinary failed:%s", name, err)
		fout.Close()
		return err
	}
	_, err = fout.WriteAt(buf, int64(blk.SegOffset))
	fout.Close()
	if err != nil {
		common.Logger.Infof("saveBlock name=%s WriteAt failed:%s", name, err)
		return err
	}
	return nil
}

func getBlockBuffer(datype string) []byte {
	if datype == gData_RIDX {
		return gRidxPool.Get().([]byte)
	} else if datype == gData_IDX {
		return gIdxPool.Get().([]byte)
	} else {
		return gObjPool.Get().([]byte)
	}
}

func putBlockBuff(datype string, buf any) {
	if datype == gData_RIDX {
		gRidxPool.Put(buf)
	} else if datype == gData_IDX {
		gIdxPool.Put(buf)
	} else {
		gObjPool.Put(buf)
	}
}

func getTypeSize(datype string) uint32 {
	if datype == gData_RIDX {
		return gBLK_RIDX_SIZE
	} else if datype == gData_IDX {
		return gBLK_IDX_SIZE
	} else {
		return gBLK_OBJ_SIZE
	}
}

func getCacheType(datype string) int {
	if datype == gData_RIDX {
		return gCache_RIDX
	} else if datype == gData_IDX {
		return gCache_IDX
	} else {
		return gCache_VAL
	}
}
