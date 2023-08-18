package impl

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/tao/faststore/api"
	"github.com/tao/faststore/common"
)

func (lg *fstLoggerImpl) Append(key string, value *api.FstTsdbValue) error {
	if err := lg.openForWr(); err != nil {
		return err
	}
	tlv := TsdbLogValue{Key: key, Timestamp: value.Timestamp, Data: value.Data}
	out, err := tlv.MarshalBinary()
	if err != nil {
		return nil
	}
	outLen := uint32(len(out))
	err = lg.checkAndFlush(outLen)
	if err != nil {
		return nil
	}
	putIntToB(lg.cache[lg.cacheOff:], outLen)
	lg.cacheOff += gBLK_V_H_LEN
	bcopy(lg.cache, out, lg.cacheOff, 0, outLen)
	lg.cacheOff += outLen
	return nil
}
func (lg *fstLoggerImpl) ForEach(call func(key string, value *api.FstTsdbValue) error) error {
	dir := fmt.Sprintf("%s/%s/dlog", lg.dir, lg.table)
	tailFile, _ := findTailFile(dir, lg.table)
	if tailFile == "" {
		common.Logger.Infof("tailFile=%s is empty", tailFile)
		return errors.New("dlog is empty")
	}
	number, _ := getTailNumber(tailFile)
	common.Logger.Infof("tail=%s,number=%d", tailFile, number)
	if number <= 0 {
		return errors.New("format error")
	}
	cache := make([]byte, gBLK_OBJ_SIZE)
	lenBuf := make([]byte, gBLK_V_H_LEN)
	tail := 1
	for tail <= number {
		fileName := fmt.Sprintf("%s/%s/dlog/%s-%04d.log", lg.dir, lg.table, lg.table, tail)
		tail += 1
		in, err := os.OpenFile(fileName, os.O_RDONLY, 0755)
		if err != nil {
			common.Logger.Warnf("file:%s, open failed:%s", fileName, err)
			return err
		}
		info, err := in.Stat()
		if err != nil {
			in.Close()
			return err
		}
		fileOff := int(info.Size())
		readOff := 0
		common.Logger.Infof("Proccess file=%s, fileOff=%d", fileName, fileOff)
		for readOff < fileOff {
			_, err = in.Read(lenBuf)
			if err != nil {
				common.Logger.Warnf("file:%s, read:%s", fileName, err)
				in.Close()
				return err
			}
			rLen := getIntFromB(lenBuf)
			if rLen >= gBLK_OBJ_SIZE {
				common.Logger.Warnf("file:%s, getIntFromB:%d is error", fileName, rLen)
				in.Close()
				return errors.New("rLen error")
			}
			s := cache[0:rLen]
			_, err := in.Read(s)
			if err != nil {
				common.Logger.Warnf("file:%s, read:%s", fileName, err)
				in.Close()
				return err
			}
			off := uint32(0)
			tslv := TsdbLogValue{}
			fsv := api.FstTsdbValue{}
			for off < rLen {
				vL := getIntFromB(s[off:])
				off += gBLK_V_H_LEN
				if (off + vL) > rLen {
					common.Logger.Warnf("file:%s, getIntFromB:%d at off=%d,rLen=%d failed", fileName, rLen, (off - gBLK_V_H_LEN), rLen)
					in.Close()
					return errors.New("vL error")
				}
				err = tslv.unmarshal(s[off:], int(vL))
				off += vL
				if err != nil {
					common.Logger.Warnf("file:%s, unmarshal failed:%s", fileName, rLen)
					in.Close()
					return err
				}
				fsv.Timestamp = tslv.Timestamp
				fsv.Data = tslv.Data
				err = call(tslv.Key, &fsv)
				if err != nil {
					common.Logger.Warnf("file:%s, docall failed:%d", fileName, rLen)
					in.Close()
					return err
				}
			} //end proccess a buffer
			readOff += int(rLen + gBLK_V_H_LEN)
		} // end read a file
		in.Close()
	}
	return nil
}

func (lg *fstLoggerImpl) Close() {
	if lg.ios == nil {
		return
	}
	if lg.cacheOff > gBLK_V_H_LEN {
		putIntToB(lg.cache, lg.cacheOff-gBLK_V_H_LEN)
		n, err := lg.ios.Write(lg.cache[0:lg.cacheOff])
		if err != nil {
			common.Logger.Warnf("put file=%s, fileOff=%d, readOff=%d,len=%d,error:%s", lg.tailName, lg.fileOff, lg.cacheOff, n, err)
		}
		lg.cache = nil
	}
	lg.ios.Close()
	lg.ios = nil
	lg.fileOff = 0
	lg.cacheOff = gBLK_V_H_LEN
}

func (lg *fstLoggerImpl) openForWr() error {
	if lg.ios != nil {
		return nil
	}
	dir := fmt.Sprintf("%s/%s/dlog", lg.dir, lg.table)
	os.MkdirAll(dir, 0755)
	lg.cache = make([]byte, gBLK_OBJ_SIZE)
	lg.cacheOff = gBLK_V_H_LEN
	tailFile, err := findTailFile(dir, lg.table)
	if err != nil {
		return err
	}
	if tailFile == "" {
		tailFile = fmt.Sprintf("%s-%04d.log", lg.table, 1)
		common.Logger.Infof("Start to write:%s", tailFile)
	}
	lg.tailName = tailFile
	fileName := fmt.Sprintf("%s/%s", dir, tailFile)
	lg.ios, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return err
	}
	info, err := lg.ios.Stat()
	if err != nil {
		return err
	}
	lg.fileOff = uint32(info.Size())
	return nil
}

func (lg *fstLoggerImpl) checkAndFlush(outLen uint32) error {
	dLen := outLen + gBLK_V_H_LEN
	if dLen+lg.cacheOff <= gBLK_OBJ_SIZE {
		return nil
	}
	if (lg.fileOff + lg.cacheOff + dLen) > gBLK_FILE_SZ {
		lg.ios.Close()
		number, _ := getTailNumber(lg.tailName)
		if number <= 0 {
			return errors.New("tail number error")
		}
		lg.tailName = fmt.Sprintf("%s-%04d.log", lg.table, (number + 1))
		common.Logger.Infof("open new file:%s", lg.tailName)
		fileName := fmt.Sprintf("%s/%s/dlog/%s", lg.dir, lg.table, lg.tailName)
		out, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
		if err != nil {
			return err
		}
		lg.ios = out
		lg.fileOff = 0
	}
	common.Logger.Debugf("file:%s, flush off=%d and off=%d", lg.tailName, lg.cacheOff, lg.fileOff)
	putIntToB(lg.cache, (lg.cacheOff - gBLK_V_H_LEN))
	n, err := lg.ios.Write(lg.cache[0:lg.cacheOff])
	if err != nil {
		common.Logger.Warnf("put file=%s, fileOff=%d, readOff=%d,len=%d,error:%s", lg.tailName, lg.fileOff, lg.cacheOff, n, err)
		return err
	}
	lg.fileOff += lg.cacheOff
	lg.cacheOff = gBLK_V_H_LEN
	return nil
}

func getTailNumber(name string) (int, error) {
	start := 0
	for {
		if name[start] >= '1' && name[start] <= '9' {
			break
		}
		start++
	}
	sLen := len(name)
	if start >= sLen {
		return -1, errors.New("format error")
	}
	number := 0
	for start < sLen {
		if name[start] < '1' || name[start] > '9' {
			break
		}
		number = number*10 + int(name[start]-'0')
		start++
	}
	return number, nil
}

func findTailFile(dir string, sufix string) (string, error) {
	tailFile := ""
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasPrefix(d.Name(), sufix) {
			return nil
		}
		if tailFile == "" || strings.Compare(d.Name(), tailFile) > 0 {
			tailFile = d.Name()
		}
		return nil
	})
	if err != nil {
		return tailFile, err
	}
	common.Logger.Infof("Find dir=%s, sufix=%s, tail=%s", dir, sufix, tailFile)
	return tailFile, nil
}
