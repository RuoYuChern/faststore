package impl

import (
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
	gData_RIDX = "ridx"
	gData_IDX  = "idx"
	gData_VAL  = "leaf"
)

func (ta *tsdbAppender) append(value *api.FstTsdbValue) error {
	err := ta.getRef()
	if err != nil {
		common.Logger.Infof("value=%d, failed:%s", value.Timestamp, err)
		return err
	}
	err = ta.getTailRIdx()
	if err != nil {
		common.Logger.Infof("value=%d, failed:%s", value.Timestamp, err)
		return err
	}
	// 只支持追加写
	if ta.lastRidx != nil && value.Timestamp < int64(ta.lastRidx.High) {
		return nil
	}
	return nil
}

func (ta *tsdbAppender) getRef() error {
	if ta.topRef == nil {
		ta.topRef = &BlockRefer{}
		err := getTsData(ta.impl.table, ta.impl.symbol, ta.topRef)
		if err == nil {
			ref, err := alloc(ta.impl.dataDir, ta.impl.table, gData_RIDX)
			if err != nil {
				return err
			}
			ta.topRef = ref
		}
	}

	return nil
}

func (ta *tsdbAppender) getTailRIdx() error {
	if ta.lastRidx != nil {
		return nil
	}
	if ta.topRef.BlkLen == 0 {
		ta.tailRef = ta.topRef
	} else {
		block := &Block{}
		addr := &BlockAddr{BlkNo: ta.topRef.BlkNo, BlkOffset: ta.topRef.BlkOffset}
		for {
			err := getBlock(addr, ta.impl.dataDir, ta.impl.table, gData_RIDX, block)
			if err != nil {
				common.Logger.Infof("getBlock failed:%s", err)
				return err
			}
			if block.BH.Next.BlkNo != 0 {
				addr.BlkNo = block.BH.Next.BlkNo
				addr.BlkOffset = block.BH.Next.BlkOffset
				continue
			}
			buf := make([]byte, gTSDB_RIDX_LEN)
			bh := &block.BH
			ta.tailRef = &BlockRefer{BlkNo: addr.BlkNo, BlkOffset: addr.BlkOffset, BlkLen: bh.Len}
			bcopy(buf, block.Data, 0, int(bh.Len)-gTSDB_RIDX_LEN, gTSDB_RIDX_LEN)
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

func alloc(dir, table, datype string) (*BlockRefer, error) {
	gAlocLock.Lock()
	ba := &BlockAloc{BlkNo: 0, BlkLen: 0}
	ref := &BlockRefer{}
	err := getTsData(table, fmt.Sprintf("tsdb.%s.spb", datype), ba)
	datSize := getTypeSize(datype)
	if err == nil {
		if (int(ba.BlkLen) + datSize) <= gBLK_FILE_SZ {
			ref.BlkNo = ba.BlkNo
			ref.BlkLen = 0
			ref.BlkOffset = ba.BlkLen
			ba.BlkLen += uint32(datSize)
			err = saveTsData(table, fmt.Sprintf("tsdb.%s.spb", datype), ba)
			gAlocLock.Unlock()
			if err != nil {
				return nil, err
			}
			return ref, nil
		} else {
			//需要重新分配
			ba.BlkNo = ba.BlkNo + 1
			ref.BlkNo = ba.BlkNo
			ref.BlkLen = 0
			ref.BlkOffset = ba.BlkLen
			ba.BlkLen = uint32(datSize)
		}
	} else {
		//第一块
		ba.BlkNo = uint32(1)
		ref.BlkNo = ba.BlkNo
		ref.BlkLen = 0
		ref.BlkOffset = ba.BlkLen
		ba.BlkLen = uint32(datSize)
	}
	err = newBlock(ba.BlkNo, dir, table, datype)
	if err != nil {
		gAlocLock.Unlock()
		return nil, err
	}
	err = saveTsData(table, fmt.Sprintf("tsdb.%s.spb", datype), ba)
	if err != nil {
		gAlocLock.Unlock()
		return nil, err
	}
	gAlocLock.Unlock()
	return ref, nil
}

func newBlock(blockNo uint32, dir, table, datype string) error {
	os.MkdirAll(fmt.Sprintf("%s/tsdb/%s", dir, table), 0755)
	name := fmt.Sprintf("%s/tsdb/%s/%d_.%s", dir, table, blockNo, datype)
	fout, err := os.OpenFile(name, os.O_CREATE, 0755)
	if err != nil {
		common.Logger.Infof("newBlock name=%s open failed:%s", name, err)
		return err
	}
	err = fout.Truncate(int64(gBLK_FILE_SZ))
	fout.Close()
	if err != nil {
		common.Logger.Infof("newBlock name=%s truncate failed:%s", name, err)
		return err
	}
	return nil
}

func getBlock(blockNo *BlockAddr, dir, table, datype string, data FsData) error {
	name := fmt.Sprintf("%s/tsdb/%s/%d_.%s", dir, table, blockNo.BlkNo, datype)
	fout, err := os.OpenFile(name, os.O_RDWR, 0755)
	if err != nil {
		common.Logger.Infof("getBlock name=%s open failed:%s", name, err)
		return err
	}
	buf := getBlockBuffer(datype)
	_, err = fout.ReadAt(buf, int64(blockNo.BlkOffset))
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

func getTypeSize(datype string) int {
	if datype == gData_RIDX {
		return gBLK_RIDX_SIZE
	} else if datype == gData_IDX {
		return gBLK_IDX_SIZE
	} else {
		return gBLK_OBJ_SIZE
	}
}
