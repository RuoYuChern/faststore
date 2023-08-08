package impl

import (
	"errors"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
	"github.com/tao/faststore/api"
	"github.com/tao/faststore/common"
)

var blotDb *bolt.DB

func StartDb(c *api.TsdbConf) error {
	os.MkdirAll(fmt.Sprintf("%s/blot", c.DataDir), 0755)
	dbName := fmt.Sprintf("%s/blot/blot.db", c.DataDir)
	db, err := bolt.Open(dbName, 0600, nil)
	if err != nil {
		common.Logger.Infof("Open db:%s, failed:%s", dbName, err)
		return err
	}
	blotDb = db
	return nil
}

func StopDb() {
	if blotDb != nil {
		blotDb.Close()
	}
}

func getTsData(table, key string, data FsData) error {
	buf, err := getBValue(table, key)
	if err != nil {
		return err
	}
	err = data.UnmarshalBinary(buf)
	if err != nil {
		common.Logger.Infof("UnmarshalBinary table=%s,key=%s,failed:%s", table, key, err)
	}
	return err
}

func saveTsData(table, key string, data FsData) error {
	buf, err := data.MarshalBinary()
	if err != nil {
		common.Logger.Infof("MarshalBinary table=%s,key=%s,failed:%s", table, key, err)
		return err
	}
	err = setBValue(table, key, buf)
	if err != nil {
		common.Logger.Infof("setBValue table=%s,key=%s,failed:%s", table, key, err)
		return err
	}
	return err
}

func getBValue(table, key string) ([]byte, error) {
	var value []byte
	err := blotDb.View(func(tx *bolt.Tx) error {
		buck := tx.Bucket([]byte(table))
		if buck == nil {
			return errors.New("find none")
		}
		bKey := []byte(key)
		value = buck.Get(bKey)
		if value == nil {
			return errors.New("find none")
		}
		return nil
	})
	return value, err
}

func setBValue(table, key string, value []byte) error {
	err := blotDb.Update(func(tx *bolt.Tx) error {
		buck, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			common.Logger.Infof("create bucket %s failed:%s", table, err)
			return err
		}
		bKey := []byte(key)
		err = buck.Put(bKey, value)
		if err != nil {
			common.Logger.Infof("put key:%s, failed:%s", key, err)
			return err
		}
		return nil
	})
	return err
}
