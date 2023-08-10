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
	buf := make([]byte, (8 + bLen))
	lwd := binary.LittleEndian
	lwd.PutUint64(buf, uint64(br.Timestamp))
	bcopy(buf, br.Data, 8, 0, uint32(bLen))
	return buf, nil
}

func (br *TsdbValue) UnmarshalBinary(data []byte) error {
	bLen := len(data) - 8
	if bLen <= 0 {
		return errors.New("out of range")
	}
	lwd := binary.LittleEndian
	br.Timestamp = int64(lwd.Uint64(data))
	br.Data = make([]byte, bLen)
	bcopy(br.Data, data, 0, 8, uint32(bLen))
	return nil
}

func (br *Block) MarshalBinary() ([]byte, error) {
	bLen := len(br.Data)
	buf := make([]byte, (int(gBH_LEN) + bLen))

	bh := &br.BH
	lwd := binary.LittleEndian
	lwd.PutUint32(buf, bh.Pre.SegNo)
	lwd.PutUint32(buf[4:], bh.Pre.SegOffset)
	lwd.PutUint32(buf[12:], bh.Next.SegNo)
	lwd.PutUint32(buf[16:], bh.Next.SegOffset)
	lwd.PutUint32(buf[24:], bh.Len)
	bcopy(buf, br.Data, gBH_LEN, 0, uint32(bLen))
	return buf, nil
}

func (br *Block) UnmarshalBinary(data []byte) error {
	bLen := len(data) - int(gBH_LEN)
	if bLen <= 0 {
		return errors.New("out of range")
	}
	bh := &br.BH
	err := bh.UnmarshalBinary(data)
	if err != nil {
		return nil
	}
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
