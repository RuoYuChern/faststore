package main

import (
	"bytes"
	"log"
	"math/rand"
	"time"

	"github.com/tao/faststore"
	"github.com/tao/faststore/api"
	"github.com/tao/faststore/impl"
)

var gNow = int64(1000000)
var baiWan int64 = 1000000
var gYiyi int64 = 30 * baiWan

func testWr() {
	now := gNow
	var times int64 = 0
	data := []byte("Hello world")
	fval := api.FstTsdbValue{Timestamp: now, Data: data}
	call := faststore.FsTsdbGet("crpto", "btc_usd")
	var yiyi int64 = gYiyi
	ios := 0
	for times <= yiyi {
		fval.Timestamp = now + times
		err := call.Append(&fval)
		if err != nil {
			log.Printf("Append key=%d, err:%s", fval.Timestamp, err)
			break
		}
		ios++
		if ios >= 100000 {
			log.Printf("times:%d", times)
			ios = 0
		}
		times += 1

	}
	call.Close()
}

func testSingle() {
	call := faststore.FsTsdbGet("crpto", "btc_usd")
	end := gNow + 10000
	items, err := call.GetLastN(end, 200)
	if err != nil {
		log.Printf("errors:%s", err)
		return
	}
	log.Printf("End:%d, Len:%d", end, items.Len())
	for f := items.Front(); f != nil; f = f.Next() {
		v := f.Value.(*api.FstTsdbValue)
		log.Printf("Key:%d, value:%s", v.Timestamp, string(v.Data))
	}
}

func testGetRange() {
	tlv := impl.TsdbLogValue{Key: "btc_usd", Timestamp: gNow, Data: []byte("Hello world")}
	buf, err := tlv.MarshalBinary()
	if err != nil {
		log.Printf("error:%s", err)
		return
	}
	otlv := &impl.TsdbLogValue{}
	err = otlv.UnmarshalBinary(buf)
	if err != nil {
		log.Printf("error:%s", err)
		return
	}
	log.Printf("Key=%s, time=%d, value=%s", otlv.Key, otlv.Timestamp, string(otlv.Data))
	call := faststore.FsTsdbGet("crpto", "btc_usd")
	low := int64(13153624)
	end := int64(13153624) + 890
	items, err := call.GetBetween(low, end, 0)
	if err != nil {
		log.Printf("error:%s", err)
		return
	}
	first := items.Front().Value.(*api.FstTsdbValue)
	tail := items.Back().Value.(*api.FstTsdbValue)
	log.Printf("low=%d, high:=%d", first.Timestamp, tail.Timestamp)
	if (tail.Timestamp != end) || (first.Timestamp != low) {
		log.Printf("GetBetween range error:%d - %d != 1000", tail.Timestamp, first.Timestamp)
		return
	}
	call.Close()
}

func testMGet() {
	call := faststore.FsTsdbGet("crpto", "btc_usd")
	low := gNow + 1000
	high := gNow + gYiyi
	rnd := rand.New(rand.NewSource(time.Now().UnixMilli()))
	for low < high {
		d := rnd.Int63n(1001)
		if d == 0 {
			d = 1
		}
		end := low + d - 1
		if end > high {
			end = high
		}
		items, err := call.GetBetween(low, end, 0)
		if err != nil {
			log.Printf("error:%s", err)
			break
		}
		first := items.Front().Value.(*api.FstTsdbValue)
		tail := items.Back().Value.(*api.FstTsdbValue)
		if (tail.Timestamp != end) || (first.Timestamp != low) {
			log.Printf("GetBetween: tail=%d != %d, low=%d !=%d, d=%d", tail.Timestamp, end, first.Timestamp, low, d)
			break
		}
		low = end
	}
	call.Close()
}

func testGn() {
	now := gNow
	var yiyi int64 = gYiyi
	blk := impl.Block{BH: impl.BlockHeader{}, Data: []byte("Hello world")}
	blk.BH.Len = 28
	out, err := blk.MarshalBinary()
	if err != nil {
		log.Printf("errors:%s", err)
		return
	}
	nblk := &impl.Block{}
	err = nblk.UnmarshalBinary(out)
	if err != nil {
		log.Printf("errors:%s", err)
		return
	}
	log.Printf("%+v", blk.BH)
	log.Printf("%+v", nblk.BH)
	call := faststore.FsTsdbGet("crpto", "btc_usd")
	ios := 0
	data := []byte("Hello world")
	for times := int64(1000); times < yiyi; times += 1 {
		items, err := call.GetLastN(now+times, 1000)
		if err != nil {
			log.Printf("GatLastN %d err:%s", now+times, err)
			break
		}
		if items.Len() != 1000 {
			log.Printf("GatLastN %d len err:%d", now+times, items.Len())
			break
		}
		b := false
		first := items.Front().Value.(*api.FstTsdbValue)
		tail := items.Back().Value.(*api.FstTsdbValue)
		if (tail.Timestamp - first.Timestamp + 1) != 1000 {
			log.Printf("GatLastN range error:%d - %d != 1000", tail.Timestamp, first.Timestamp)
			break
		}
		for f := items.Front(); f != nil; f = f.Next() {
			v := f.Value.(*api.FstTsdbValue)
			if !bytes.Equal(data, v.Data) {
				log.Printf("key: %d, data error: %d != %d", v.Timestamp, len(v.Data), len(data))
				b = true
				break
			}
		}
		if b {
			break
		}
		ios++
		if ios >= 100000 {
			log.Printf("times:%d", times)
			ios = 0
		}
	}
	call.Close()
}

func main() {
	conf := api.TsdbConf{Level: "info", File: "../log/tao.log", MaxSize: 50, MaxBackups: 10, MaxAge: 1, Env: "dev", DataDir: "../data/fstore"}
	faststore.Start(&conf)
	c := 'm'
	switch c {
	case 'w':
		testWr()
	case 'g':
		testGn()
	case 's':
		testSingle()
	case 'r':
		testGetRange()
	case 'm':
		testMGet()
	}
	faststore.Stop()
}
