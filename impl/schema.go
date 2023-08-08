package impl

import (
	"encoding/binary"
	"errors"
)

var (
	gBLK_RIDX_SIZE = (8 << 10)
	gBLK_IDX_SIZE  = (16 << 10)
	gBLK_OBJ_SIZE  = (32 << 10)
	gBLK_FILE_SZ   = (256 << 20)

	gBAL_LEN       = 8
	gBR_LEN        = 12
	gBA_LEN        = 8
	gBH_LEN        = 20
	gBLK_V_H_LEN   = 4
	gTSDB_RIDX_LEN = 28
	gTSDB_IDX_LEN  = 16
)

type FsData interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
}

type BlockAloc struct {
	FsData
	BlkNo  uint32
	BlkLen uint32
}

type BlockRefer struct {
	FsData
	BlkNo     uint32
	BlkOffset uint32
	BlkLen    uint32
}

type BlockAddr struct {
	FsData
	BlkNo     uint32
	BlkOffset uint32
}

type BlockHeader struct {
	FsData
	Pre  BlockAddr
	Next BlockAddr
	Len  uint32
}

type TsdbRangIndex struct {
	FsData
	Low   uint64
	High  uint64
	Items uint32
	Addr  BlockAddr
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
	lwd.PutUint32(buf, br.BlkNo)
	lwd.PutUint32(buf[4:], br.BlkLen)
	return buf, nil
}
func (br *BlockAloc) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.BlkNo = lwd.Uint32(data)
	br.BlkLen = lwd.Uint32(data[4:])
	return nil
}

func (br *BlockRefer) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gBR_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint32(buf, br.BlkNo)
	lwd.PutUint32(buf[4:], br.BlkOffset)
	lwd.PutUint32(buf[8:], br.BlkLen)
	return buf, nil
}
func (br *BlockRefer) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.BlkNo = lwd.Uint32(data)
	br.BlkOffset = lwd.Uint32(data[4:])
	br.BlkLen = lwd.Uint32(data[8:])
	return nil
}

func (br *BlockAddr) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gBA_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint32(buf, br.BlkNo)
	lwd.PutUint32(buf[4:], br.BlkOffset)
	return buf, nil
}

func (br *BlockAddr) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.BlkNo = lwd.Uint32(data)
	br.BlkOffset = lwd.Uint32(data[4:])
	return nil
}

func (br *BlockHeader) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gBH_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint32(buf, br.Pre.BlkNo)
	lwd.PutUint32(buf[4:], br.Pre.BlkOffset)
	lwd.PutUint32(buf[8:], br.Next.BlkNo)
	lwd.PutUint32(buf[12:], br.Next.BlkOffset)
	lwd.PutUint32(buf[16:], br.Len)
	return buf, nil
}

func (br *BlockHeader) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.Pre.BlkNo = lwd.Uint32(data)
	br.Pre.BlkOffset = lwd.Uint32(data[4:])
	br.Next.BlkNo = lwd.Uint32(data[8:])
	br.Next.BlkOffset = lwd.Uint32(data[12:])
	br.Len = lwd.Uint32(data[16:])
	return nil
}

func (br *TsdbRangIndex) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gTSDB_RIDX_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint64(buf, br.Low)
	lwd.PutUint64(buf[8:], br.High)
	lwd.PutUint32(buf[16:], br.Items)
	lwd.PutUint32(buf[20:], br.Addr.BlkNo)
	lwd.PutUint32(buf[24:], br.Addr.BlkOffset)
	return buf, nil
}

func (br *TsdbRangIndex) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.Low = lwd.Uint64(data)
	br.High = lwd.Uint64(data[8:])
	br.Items = lwd.Uint32(data[16:])
	br.Addr.BlkNo = lwd.Uint32(data[20:])
	br.Addr.BlkOffset = lwd.Uint32(data[24:])
	return nil
}

func (br *TsdbIndex) MarshalBinary() ([]byte, error) {
	buf := make([]byte, gTSDB_IDX_LEN)
	lwd := binary.LittleEndian
	lwd.PutUint64(buf, br.Key)
	lwd.PutUint32(buf[8:], br.Addr.BlkNo)
	lwd.PutUint32(buf[12:], br.Addr.BlkOffset)
	return buf, nil
}

func (br *TsdbIndex) UnmarshalBinary(data []byte) error {
	lwd := binary.LittleEndian
	br.Key = lwd.Uint64(data)
	br.Addr.BlkNo = lwd.Uint32(data[8:])
	br.Addr.BlkOffset = lwd.Uint32(data[12:])
	return nil
}

func (br *TsdbValue) MarshalBinary() ([]byte, error) {
	bLen := len(br.Data)
	buf := make([]byte, (8 + bLen))
	lwd := binary.LittleEndian
	lwd.PutUint64(buf, uint64(br.Timestamp))
	bcopy(buf, br.Data, 8, 0, bLen)
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
	bcopy(br.Data, data, 0, 8, bLen)
	return nil
}

func (br *Block) MarshalBinary() ([]byte, error) {
	bLen := len(br.Data)
	buf := make([]byte, (gBH_LEN + bLen))

	bh := &br.BH
	lwd := binary.LittleEndian
	lwd.PutUint32(buf, bh.Pre.BlkNo)
	lwd.PutUint32(buf[4:], bh.Pre.BlkOffset)
	lwd.PutUint32(buf[12:], bh.Next.BlkNo)
	lwd.PutUint32(buf[16:], bh.Next.BlkOffset)
	lwd.PutUint32(buf[24:], bh.Len)
	bcopy(buf, br.Data, gBH_LEN, 0, bLen)
	return buf, nil
}

func (br *Block) UnmarshalBinary(data []byte) error {
	bLen := len(data) - gBH_LEN
	if bLen <= 0 {
		return errors.New("out of range")
	}
	bh := &br.BH
	err := bh.UnmarshalBinary(data)
	if err != nil {
		return nil
	}

	bcopy(br.Data, data, 0, gBH_LEN, bLen)
	return nil
}
