package impl

import (
	"encoding/binary"
	"errors"
)

var (
	gBLK_RIDX_SIZE = uint32(8 << 10)
	gBLK_IDX_SIZE  = uint32(16 << 10)
	gBLK_OBJ_SIZE  = uint32(32 << 10)
	gBLK_FILE_SZ   = uint32(256 << 20)

	gBAL_LEN       = uint32(8)
	gBA_LEN        = uint32(8)
	gBH_LEN        = uint32(20)
	gBLK_V_H_LEN   = uint32(4)
	gBLK_K_LEN     = 8
	gTSDB_RIDX_LEN = uint32(28)
	gTSDB_IDX_LEN  = uint32(16)
)

type FsData interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
}

type BlockAloc struct {
	FsData
	SegNo   uint32
	AlocLen uint32
}

type BlockAddr struct {
	FsData
	SegNo     uint32
	SegOffset uint32
}

type BlockHeader struct {
	FsData
	Pre  BlockAddr
	Next BlockAddr
	Len  uint32
}

type TsdbRangIndex struct {
	FsData
	Low  uint64
	High uint64
	Off  uint32
	Addr BlockAddr
}

type TsdbIndex struct {
	FsData
	Key  uint64
	Addr BlockAddr
}

type TsdbValue struct {
	FsData
	Timestamp int64
	Data      []byte
}

type TsdbLogValue struct {
	FsData
	Key       string
	Timestamp int64
	Data      []byte
}

type Block struct {
	FsData
	BH   BlockHeader
	Data []byte
}

// impl
func (br *BlockAloc) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gBAL_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint32(buf, br.SegNo)
	lwd.PutUint32(buf[4:], br.AlocLen)
	return buf, nil
}
func (br *BlockAloc) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.SegNo = lwd.Uint32(data)
	br.AlocLen = lwd.Uint32(data[4:])
	return nil
}

func (br *BlockAddr) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gBA_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint32(buf, br.SegNo)
	lwd.PutUint32(buf[4:], br.SegOffset)
	return buf, nil
}

func (br *BlockAddr) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.SegNo = lwd.Uint32(data)
	br.SegOffset = lwd.Uint32(data[4:])
	return nil
}

func (br *BlockHeader) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gBH_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint32(buf, br.Pre.SegNo)
	lwd.PutUint32(buf[4:], br.Pre.SegOffset)
	lwd.PutUint32(buf[8:], br.Next.SegNo)
	lwd.PutUint32(buf[12:], br.Next.SegOffset)
	lwd.PutUint32(buf[16:], br.Len)
	return buf, nil
}

func (br *BlockHeader) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.Pre.SegNo = lwd.Uint32(data)
	br.Pre.SegOffset = lwd.Uint32(data[4:])
	br.Next.SegNo = lwd.Uint32(data[8:])
	br.Next.SegOffset = lwd.Uint32(data[12:])
	br.Len = lwd.Uint32(data[16:])
	return nil
}

func (br *TsdbRangIndex) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gTSDB_RIDX_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint64(buf, br.Low)
	lwd.PutUint64(buf[8:], br.High)
	lwd.PutUint32(buf[16:], br.Off)
	lwd.PutUint32(buf[20:], br.Addr.SegNo)
	lwd.PutUint32(buf[24:], br.Addr.SegOffset)
	return buf, nil
}

func (br *TsdbRangIndex) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.Low = lwd.Uint64(data)
	br.High = lwd.Uint64(data[8:])
	br.Off = lwd.Uint32(data[16:])
	br.Addr.SegNo = lwd.Uint32(data[20:])
	br.Addr.SegOffset = lwd.Uint32(data[24:])
	return nil
}

func (br *TsdbIndex) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gTSDB_IDX_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint64(buf, br.Key)
	lwd.PutUint32(buf[8:], br.Addr.SegNo)
	lwd.PutUint32(buf[12:], br.Addr.SegOffset)
	return buf, nil
}

func (br *TsdbIndex) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.Key = lwd.Uint64(data)
	br.Addr.SegNo = lwd.Uint32(data[8:])
	br.Addr.SegOffset = lwd.Uint32(data[12:])
	return nil
}

func (br *TsdbValue) MarshalBinary() ([]byte, error) {
	bLen := len(br.Data)
	buf := make([]byte, (gBLK_K_LEN + bLen))
	lwd := binary.LittleEndian
	lwd.PutUint64(buf, uint64(br.Timestamp))
	bcopy(buf, br.Data, uint32(gBLK_K_LEN), 0, uint32(bLen))
	return buf, nil
}

func (br *TsdbValue) UnmarshalBinary(data []byte) error {
	bLen := len(data) - gBLK_K_LEN
	if bLen <= 0 {
		return errors.New("out of range")
	}
	lwd := binary.LittleEndian
	br.Timestamp = int64(lwd.Uint64(data))
	br.Data = make([]byte, bLen)
	bcopy(br.Data, data, 0, uint32(gBLK_K_LEN), uint32(bLen))
	return nil
}

func (br *TsdbValue) unmarshal(data []byte, dLen int) error {
	bLen := dLen - gBLK_K_LEN
	if bLen <= 0 {
		return errors.New("out of range")
	}
	lwd := binary.LittleEndian
	br.Timestamp = int64(lwd.Uint64(data))
	br.Data = make([]byte, bLen)
	bcopy(br.Data, data, 0, uint32(gBLK_K_LEN), uint32(bLen))
	return nil
}

func (br *TsdbLogValue) MarshalBinary() ([]byte, error) {
	bKey := []byte(br.Key)
	kLen := uint32(len(bKey))
	bLen := 4 + len(bKey) + 8 + len(br.Data)
	outBuf := make([]byte, bLen)
	//字符串长度
	lwd := binary.LittleEndian
	lwd.PutUint32(outBuf, kLen)
	bcopy(outBuf, bKey, 4, 0, kLen)
	dOff := 4 + kLen
	lwd.PutUint64(outBuf[dOff:], uint64(br.Timestamp))
	dOff += 8
	bcopy(outBuf, br.Data, dOff, 0, uint32(len(br.Data)))
	return outBuf, nil
}

func (br *TsdbLogValue) UnmarshalBinary(data []byte) error {
	dLen := len(data)
	if dLen <= 12 {
		return errors.New("out of range")
	}
	lwd := binary.LittleEndian
	kLen := lwd.Uint32(data)
	if uint32(dLen) < (uint32(12) + kLen) {
		return errors.New("out of range")
	}
	sOff := 4
	br.Key = string(duplicate(data[sOff : sOff+int(kLen)]))
	sOff += int(kLen)
	br.Timestamp = int64(lwd.Uint64(data[sOff:]))
	sOff += 8
	if sOff >= dLen {
		return errors.New("out of range")
	}
	br.Data = make([]byte, (dLen - sOff))
	bcopy(br.Data, data, 0, uint32(sOff), uint32(dLen-sOff))
	return nil
}

func (br *Block) MarshalBinary() ([]byte, error) {
	bLen := len(br.Data)
	outBuf := make([]byte, (int(gBH_LEN) + bLen))
	bh := &br.BH
	lwd := binary.LittleEndian
	lwd.PutUint32(outBuf, bh.Pre.SegNo)
	lwd.PutUint32(outBuf[4:], bh.Pre.SegOffset)
	lwd.PutUint32(outBuf[8:], bh.Next.SegNo)
	lwd.PutUint32(outBuf[12:], bh.Next.SegOffset)
	lwd.PutUint32(outBuf[16:], bh.Len)
	bcopy(outBuf, br.Data, gBH_LEN, 0, uint32(bLen))
	return outBuf, nil
}

func (br *Block) UnmarshalBinary(data []byte) error {
	bLen := len(data) - int(gBH_LEN)
	if bLen <= 0 {
		return errors.New("out of range")
	}
	lwd := binary.LittleEndian
	br.BH.Pre.SegNo = lwd.Uint32(data)
	br.BH.Pre.SegOffset = lwd.Uint32(data[4:])
	br.BH.Next.SegNo = lwd.Uint32(data[8:])
	br.BH.Next.SegOffset = lwd.Uint32(data[12:])
	br.BH.Len = lwd.Uint32(data[16:])
	br.Data = make([]byte, bLen)
	bcopy(br.Data, data, 0, gBH_LEN, uint32(bLen))
	return nil
}

func PutIntToB(data []byte, u uint32) {
	lwd := binary.LittleEndian
	lwd.PutUint32(data, u)
}

func GetIntFromB(data []byte) uint32 {
	lwd := binary.LittleEndian
	return lwd.Uint32(data)
}

func getIdxSegOff(off uint32) uint32 {
	return ((off / gBLK_IDX_SIZE) * gBLK_IDX_SIZE)
}

func getValueSegOff(off uint32) uint32 {
	return ((off / gBLK_OBJ_SIZE) * gBLK_OBJ_SIZE)
}

func getValueBlkOff(off uint32) uint32 {
	return (off % gBLK_OBJ_SIZE) - gBH_LEN
}
