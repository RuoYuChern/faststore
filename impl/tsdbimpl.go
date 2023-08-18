package impl

import (
	"container/list"
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
	gErr_Eof    = ftsdbEoff{}
	gErr_Empty  = ftsdbEmpty{}
	gTbl_Fmt    = "%s/%s"
	gSeg_Fmt    = "%s/%s/seg_%d.%s"
)

func isError(e, t error) bool {
	if e == nil {
		return false
	}
	return (e == t)
}

// appender
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
		segOff := getIdxSegOff(addr.SegOffset)
		tRidx := &TsdbRangIndex{Low: uint64(value.Timestamp), High: uint64(value.Timestamp + 1), Off: 0, Addr: BlockAddr{SegNo: addr.SegNo, SegOffset: segOff}}
		ta.lastRidx = tRidx
	} else {
		segOff := getIdxSegOff(addr.SegOffset)
		if (ta.lastRidx.Addr.SegNo == addr.SegNo) && (segOff == ta.lastRidx.Addr.SegOffset) {
			/**没有换新的idxBlock**/
			ta.lastRidx.High = uint64(value.Timestamp + 1)
			return nil
		}
		err = ta.ridxCache.updateTail(ta.lastRidx)
		if err != nil {
			return err
		}
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
	ta.idxCache = allocWrCache(gData_IDX, ta.impl, idxAdr, blk)
	idxItem := &TsdbIndex{}
	err = idxItem.UnmarshalBinary(blk.Data[(blk.BH.Len - uint32(gTSDB_IDX_LEN)):])
	if err != nil {
		return err
	}
	datAddr := &BlockAddr{SegNo: idxItem.Addr.SegNo, SegOffset: getValueSegOff(idxItem.Addr.SegOffset)}
	datBlk := &Block{}
	err = loadBlock(datAddr, ta.impl.dataDir, ta.impl.table, gData_VAL, datBlk)
	if err != nil {
		return err
	}
	ta.datCache = allocWrCache(gData_VAL, ta.impl, datAddr, datBlk)
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
			ta.ridxCache = allocWrCache(gData_RIDX, ta.impl, tailAdr, block)
			bcopy(buf, block.Data, 0, bh.Len-gTSDB_RIDX_LEN, gTSDB_RIDX_LEN)
			ridx := &TsdbRangIndex{}
			err = ridx.UnmarshalBinary(buf)
			if err != nil {
				common.Logger.Infof("UnmarshalBinary failed:%s", err)
				return err
			}
			ta.lastRidx = ridx
			common.Logger.Infof("lastRidx:%+v", ta.lastRidx)
			break
		}
	}
	return nil
}

func (tq *tsdbQuery) getLastN(key int64, limit int) (*list.List, error) {
	err := tq.findTidOff(key)
	if err != nil {
		return nil, err
	}
	itemList := list.New()
	start := uint32(0)
	for itemList.Len() < limit {
		tvList := list.New()
		err := tq.datCache.bachReadTo(start, uint64(key), tvList)
		if err != nil {
			return nil, err
		}
		for b := tvList.Back(); b != nil; b = b.Prev() {
			if itemList.Len() >= limit {
				break
			}
			tv := b.Value.(*TsdbValue)
			itemList.PushFront(&api.FstTsdbValue{Timestamp: tv.Timestamp, Data: tv.Data})
		}
		left := limit - itemList.Len()
		if left <= 0 {
			break
		}
		err = tq.datCache.toPre()
		if err != nil {
			if isError(err, gErr_Eof) {
				break
			}
			return nil, err
		}
	}
	return itemList, nil
}
func (tq *tsdbQuery) getBetween(low, high int64, offset int) (*list.List, error) {
	if tq.offset > offset {
		common.Logger.Infof("offset = %d < lastOffset =%d", offset, tq.offset)
		return nil, errors.New("offset error")
	}
	err := tq.findTidOff(low)
	if err != nil {
		return nil, err
	}
	itemList := list.New()
	tv := &TsdbValue{}
	for itemList.Len() < api.DEF_LIMIT {
		err = tq.datCache.forward(tv)
		if isError(err, gErr_Eof) {
			break
		}
		if err != nil {
			return nil, err
		}
		if tv.Timestamp < low {
			continue
		}
		if tq.offset < offset {
			tq.offset++
			continue
		}
		if tv.Timestamp > high {
			break
		}
		fv := &api.FstTsdbValue{Timestamp: tv.Timestamp, Data: tv.Data}
		itemList.PushBack(fv)
	}
	if itemList.Len() == 0 {
		common.Logger.Infof("low=%d, high=%d is empty", low, high)
		return nil, gErr_Empty
	}
	return itemList, nil
}

func (tq *tsdbQuery) findTidOff(key int64) error {
	if err := tq.findBlkRidx(key); err != nil {
		return err
	}

	if err := tq.findBlkIdx(key); err != nil {
		return err
	}

	if tq.datCache != nil {
		return nil
	}
	off := getValueBlkOff(tq.tIdx.Addr.SegOffset)
	addr := BlockAddr{SegNo: tq.tIdx.Addr.SegNo, SegOffset: getValueSegOff(tq.tIdx.Addr.SegOffset)}
	blk := &Block{}
	err := loadBlock(&addr, tq.impl.dataDir, tq.impl.table, gData_VAL, blk)
	if err != nil {
		return err
	}
	tq.datCache = &tsdbRDCache{blkSize: gBLK_OBJ_SIZE, readOff: off, dataType: gData_VAL, impl: tq.impl, block: blk}
	return nil
}

func (tq *tsdbQuery) findBlkIdx(key int64) error {
	if tq.tIdx != nil {
		return nil
	}
	blk := &Block{}
	err := loadBlock(&tq.tRidx.Addr, tq.impl.dataDir, tq.impl.table, gData_IDX, blk)
	if err != nil {
		return err
	}
	off := findIdxOff(blk, uint64(key))
	if off < 0 {
		return gErr_Empty
	}
	tq.tIdx = &TsdbIndex{}
	err = tq.tIdx.UnmarshalBinary(blk.Data[off:])
	if err != nil {
		return err
	}
	return nil
}

func (tq *tsdbQuery) findBlkRidx(key int64) error {
	if tq.tRidx != nil {
		return nil
	}
	topRef := &BlockAddr{}
	err := getTsData(tq.impl.table, tq.impl.symbol, topRef)
	if err != nil {
		common.Logger.Infof("Get tsdata table=%s,symbol=%s failed:%s", tq.impl.table, tq.impl.symbol, err)
		return err
	}
	block := &Block{}
	addr := BlockAddr{SegNo: topRef.SegNo, SegOffset: topRef.SegOffset}
	for {
		if addr.SegNo == 0 {
			break
		}
		err := loadBlock(&addr, tq.impl.dataDir, tq.impl.table, gData_RIDX, block)
		if err != nil {
			common.Logger.Infof("getBlock failed:%s", err)
			return err
		}
		buf := make([]byte, gTSDB_RIDX_LEN)
		first := &TsdbRangIndex{}
		bcopy(buf, block.Data, 0, 0, gTSDB_RIDX_LEN)
		err = first.UnmarshalBinary(buf)
		if err != nil {
			common.Logger.Infof("UnmarshalBinary first failed:%s", err)
			return err
		}
		bcopy(buf, block.Data, 0, block.BH.Len-gTSDB_RIDX_LEN, gTSDB_RIDX_LEN)
		tail := &TsdbRangIndex{}
		err = tail.UnmarshalBinary(buf)
		if err != nil {
			common.Logger.Infof("UnmarshalBinary tail failed:%s", err)
			return err
		}
		// 半开闭
		if key < int64(first.Low) {
			return gErr_Empty
		}
		if key >= int64(tail.High) {
			addr.SegNo = block.BH.Next.SegNo
			addr.SegOffset = block.BH.Next.SegOffset
			continue
		}
		off := findRidxOff(block, uint64(key))
		if off < 0 {
			return gErr_Empty
		}
		tq.tRidx = &TsdbRangIndex{}
		err = tq.tRidx.UnmarshalBinary(block.Data[off:])
		if err != nil {
			return err
		}
		common.Logger.Debugf("Key:%d, range:[%d,%d), off:%d", key, tq.tRidx.Low, tq.tRidx.High, tq.tRidx.Off)
		return nil
	}
	return gErr_Empty
}

func (tq *tsdbQuery) close() {
	tq.datCache = nil
	tq.tIdx = nil
	tq.tRidx = nil
	tq.offset = 0
}

// tsdbRDCache
func (ca *tsdbRDCache) bachReadTo(start uint32, key uint64, item *list.List) error {
	off := start
	for off <= ca.readOff {
		if off >= ca.block.BH.Len {
			break
		}

		bLen := getIntFromB(ca.block.Data[off:])
		off += gBLK_V_H_LEN
		if (bLen + off) > ca.block.BH.Len {
			common.Logger.Infof("bLen=%d + readOf=%d > Len=%d", bLen, off, ca.block.BH.Len)
			return errors.New("bachReadTo len error")
		}
		tv := &TsdbValue{}
		err := tv.unmarshal(ca.block.Data[off:], int(bLen))
		off += bLen
		if err != nil {
			return err
		}
		if tv.Timestamp <= int64(key) {
			item.PushBack(tv)
		}
		if tv.Timestamp >= int64(key) {
			break
		}
	}
	return nil
}

func (ca *tsdbRDCache) toPre() error {
	if ca.block.BH.Pre.SegNo == 0 {
		return gErr_Eof
	}
	blk := &Block{}
	err := loadBlock(&ca.block.BH.Pre, ca.impl.dataDir, ca.impl.table, gData_VAL, blk)
	if err != nil {
		return err
	}
	ca.block = blk
	ca.readOff = blk.BH.Len
	return nil
}

func (ca *tsdbRDCache) forward(data *TsdbValue) error {
	if (ca.readOff + gBLK_V_H_LEN) < ca.block.BH.Len {
		bLen := getIntFromB(ca.block.Data[ca.readOff:])
		ca.readOff += gBLK_V_H_LEN
		if (bLen + ca.readOff) > ca.block.BH.Len {
			common.Logger.Infof("bLen=%d + readOf=%d > Len=%d", bLen, ca.readOff, ca.block.BH.Len)
			return errors.New("read len error")
		}
		err := data.unmarshal(ca.block.Data[ca.readOff:], int(bLen))
		ca.readOff += bLen
		return err
	}
	//read next
	if ca.block.BH.Next.SegNo == 0 {
		return gErr_Eof
	}
	blk := &Block{}
	err := loadBlock(&ca.block.BH.Next, ca.impl.dataDir, ca.impl.table, gData_VAL, blk)
	if err != nil {
		return err
	}
	ca.block = blk
	ca.readOff = 0
	bLen := getIntFromB(ca.block.Data[ca.readOff:])
	ca.readOff += gBLK_V_H_LEN
	if (bLen + ca.readOff) > ca.block.BH.Len {
		common.Logger.Infof("bLen=%d + readOf=%d > Len=%d", bLen, ca.readOff, ca.block.BH.Len)
		return errors.New("read len error")
	}
	err = data.unmarshal(ca.block.Data[ca.readOff:], int(bLen))
	ca.readOff += bLen
	return err
}

// tsdbWRCache
func (ca *tsdbWRCache) updateTail(data *TsdbRangIndex) error {
	totalLen := gTSDB_RIDX_LEN + ca.block.BH.Len + gBH_LEN
	if data.Off == 0 {
		data.Off = (ca.block.BH.Len / gTSDB_RIDX_LEN) + 1
		if totalLen > uint32(ca.blkSize) {
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
	if newLen >= ca.blkSize {
		return nil, errors.New("too long data")
	}

	if (ca.block.BH.Len + newLen) <= ca.blkSize {
		addr := ca.toCache(dLen, out)
		return addr, nil
	}

	err = ca.changeCache()
	if err != nil {
		return nil, err
	}

	if (ca.block.BH.Len + newLen) <= ca.blkSize {
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
		putIntToB(ca.block.Data[ca.block.BH.Len:], dLen)
		bcopy(ca.block.Data, out, ca.block.BH.Len+gBLK_V_H_LEN, 0, dLen)
		dLen += gBLK_V_H_LEN
	} else {
		bcopy(ca.block.Data, out, ca.block.BH.Len, 0, dLen)
	}
	/**要加上头部长度**/
	segOff := (ca.addr.SegOffset + gBH_LEN) + ca.block.BH.Len
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

func findIdxOff(blk *Block, key uint64) int {
	high := blk.BH.Len / gTSDB_IDX_LEN
	low := uint32(0)
	oHigh := high
	itemBuf := make([]byte, gTSDB_IDX_LEN)
	idx := &TsdbIndex{}
	for low < high {
		mid := (low + high) / 2
		offset := mid * gTSDB_IDX_LEN
		bcopy(itemBuf, blk.Data, 0, offset, gTSDB_IDX_LEN)
		err := idx.UnmarshalBinary(itemBuf)
		if err != nil {
			common.Logger.Infof("UnmarshalBinary failed:%s", err)
			return -1
		}
		if idx.Key == key {
			return int(offset)
		}
		if idx.Key > key {
			high = mid
		} else {
			low = mid + 1
		}
	}
	if low >= oHigh {
		common.Logger.Infof("Find key=%d, some is unexceptions: low %d >= high %d", key, low, oHigh)
		low = oHigh - 1
	}
	//这种情况下,low是最佳值
	return int(low * gTSDB_IDX_LEN)
}

func findRidxOff(blk *Block, key uint64) int {
	high := blk.BH.Len / gTSDB_RIDX_LEN
	if high == 0 {
		common.Logger.Infof("BH.Len:%d", blk.BH.Len)
		return -1
	}
	low := uint32(0)
	oHigh := high
	itemBuf := make([]byte, gTSDB_RIDX_LEN)
	ridx := &TsdbRangIndex{}
	for low < high {
		mid := (low + high) / 2
		offset := mid * gTSDB_RIDX_LEN
		bcopy(itemBuf, blk.Data, 0, offset, gTSDB_RIDX_LEN)
		err := ridx.UnmarshalBinary(itemBuf)
		if err != nil {
			common.Logger.Infof("UnmarshalBinary failed:%s", err)
			return -1
		}
		// 半开闭
		if ridx.Low <= key && key < ridx.High {
			return int(offset)
		}
		if key < ridx.Low {
			//小于左区间
			high = mid
		} else {
			//大于右区间
			low = mid + 1
		}
	}
	if low >= oHigh {
		common.Logger.Infof("some is unexceptions: low %d >= high %d", low, oHigh)
		low = oHigh - 1
	}
	//这种情况下,low是最佳值
	return int(low * gTSDB_RIDX_LEN)
}

// functions
func allocBlockByType(datype string, pre *BlockAddr, impl *fstTsdbImpl) (*tsdbWRCache, error) {
	newRef, err := alloc(impl.dataDir, impl.table, datype)
	if err != nil {
		return nil, err
	}
	return newBlockCache(datype, pre, newRef, impl), nil
}

func newBlockCache(datype string, pre, newRef *BlockAddr, impl *fstTsdbImpl) *tsdbWRCache {
	dataSize := getTypeSize(datype)
	cache := &tsdbWRCache{blkSize: dataSize, dataType: datype, cacheType: getCacheType(datype), impl: impl}
	blk := &Block{BH: BlockHeader{}, Data: make([]byte, (dataSize - gBH_LEN))}
	cache.block = blk
	cache.addr = newRef
	if pre != nil {
		blk.BH.Pre.SegNo = pre.SegNo
		blk.BH.Pre.SegOffset = pre.SegOffset
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
			common.Logger.Debugf("Alloc segment=%d, offset=%d", newBa.SegNo, newBa.SegOffset)
			return newBa, nil
		} else {
			//需要重新分配(segment)
			ba.SegNo = ba.SegNo + 1
			ba.AlocLen = uint32(datSize)
			newBa.SegNo = ba.SegNo
			newBa.SegOffset = 0
			common.Logger.Infof("Alloc segment=%d, offset=%d", newBa.SegNo, newBa.SegOffset)
		}
	} else {
		//第一块(segment)
		ba.SegNo = uint32(1)
		newBa.SegNo = ba.SegNo
		newBa.SegOffset = 0
		ba.AlocLen = uint32(datSize)
		common.Logger.Infof("Datatype=%s, Alloc segment=%d, offset=%d", datype, newBa.SegNo, newBa.SegOffset)
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

func allocWrCache(dataType string, impl *fstTsdbImpl, addr *BlockAddr, block *Block) *tsdbWRCache {
	cache := &tsdbWRCache{dataType: dataType, impl: impl, addr: addr, block: block}
	cache.blkSize = getTypeSize(dataType)
	cache.cacheType = getCacheType(dataType)
	return cache
}

func newSegment(blockNo uint32, dir, table, datype string) error {
	os.MkdirAll(fmt.Sprintf(gTbl_Fmt, dir, table), 0755)
	name := fmt.Sprintf(gSeg_Fmt, dir, table, blockNo, datype)
	fout, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0755)
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

func loadBlock(addr *BlockAddr, dir, table, datype string, data *Block) error {
	dsize := getTypeSize(datype)
	if (addr.SegOffset % dsize) != 0 {
		common.Logger.Infof("loadBlock data=%s, segment=%d, segOff=%d", datype, addr.SegNo, addr.SegOffset)
		return fmt.Errorf("offset=%d mod block size=%d =%d", addr.SegOffset, dsize, (addr.SegOffset % dsize))
	}
	name := fmt.Sprintf(gSeg_Fmt, dir, table, addr.SegNo, datype)
	fout, err := os.OpenFile(name, os.O_RDONLY, 0755)
	if err != nil {
		common.Logger.Infof("getBlock name=%s open failed:%s", name, err)
		return err
	}
	buf := getBlockBuffer(datype)
	n, err := fout.ReadAt(buf, int64(addr.SegOffset))
	fout.Close()
	if err != nil {
		common.Logger.Infof("ReadAt name=%s open failed:%s", name, err)
		putBlockBuff(datype, buf)
		return err
	}
	common.Logger.Debugf("ReadAt name=%s Off:=%d, Len:%d:%d", name, addr.SegOffset, n, dsize)
	err = data.UnmarshalBinary(buf)
	putBlockBuff(datype, buf)
	if err != nil {
		common.Logger.Infof("UnmarshalBinary name=%s open failed:%s", name, err)
		return err
	}
	return nil
}

func saveBlock(addr *BlockAddr, dir, table, datype string, data *Block) error {
	dsize := getTypeSize(datype)
	if (addr.SegOffset % dsize) != 0 {
		common.Logger.Infof("saveBlock data=%s, segment=%d, segOff=%d", datype, addr.SegNo, addr.SegOffset)
		return fmt.Errorf("offset=%d mod block size=%d =%d", addr.SegOffset, dsize, (addr.SegOffset % dsize))
	}
	name := fmt.Sprintf(gSeg_Fmt, dir, table, addr.SegNo, datype)
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
	_, err = fout.WriteAt(buf, int64(addr.SegOffset))
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
