package impl

import (
	"container/list"
	"fmt"
	"os"

	"github.com/tao/faststore/api"
)

func (lg *fstLoggerImpl) Append(key string, value *api.FstTsdbValue) error {
	if err := lg.openForWr(); err != nil {
		return err
	}
	return nil
}
func (lg *fstLoggerImpl) ForEach(call func(key string, value *api.FstTsdbValue)) error {
	return nil
}

func (lg *fstLoggerImpl) Close() {

}

func (lg *fstLoggerImpl) openForWr() error {
	if lg.ios != nil {
		return nil
	}
	dir := fmt.Sprintf("%s/%s/dlog", lg.dir, lg.table)
	os.MkdirAll(dir, 0755)
	return nil
}

func findFileList(dir string, sufix string, exclude string) (*list.List, error) {
	return nil, nil
}
