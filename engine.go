package cake_db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/go-mmap/mmap"
	"github.com/peterbourgon/diskv/v3"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const ShardSize = int64(time.Hour * 7 * 24)

type Engine struct {
	points              chan *Point
	shardGroup          sync.Map
	mu                  sync.Mutex
	keyDiskv, dataDiskv *diskv.Diskv
}

const KeyPath = "/home/wyatt/code/tmp/data/key"
const ValuePath = "/home/wyatt/code/tmp/data/value"
const TmpPath = "/home/wyatt/code/tmp/data/tmp"

func GetValuePath(s string) string {
	split := strings.Split(s, "_")
	str := ValuePath + "/"
	for _, i := range split[:2] {
		str += i + "/"
	}
	str += s
	return str
}

func New() *Engine {
	flatTransform := func(s string) []string {
		if len(s) > 2 {
			return []string{s[:2], s}
		}
		return []string{"0" + s[:1]}
	}

	// Initialize a new diskv store, rooted at "my-data-dir", with a 1MB cache.
	keyDiskv := diskv.New(diskv.Options{
		BasePath:     KeyPath,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024,
	})
	dataDiskv := diskv.New(diskv.Options{
		BasePath: ValuePath,
		Transform: func(s string) []string {
			split := strings.Split(s, "_")
			return split[:2]
		},
		CacheSizeMax: 0,
	})
	return &Engine{
		points:    make(chan *Point, 1e6),
		mu:        sync.Mutex{},
		keyDiskv:  keyDiskv,
		dataDiskv: dataDiskv,
	}
}

var once = sync.Once{}

func (e *Engine) Init() {
	once.Do(func() {
		go e.handleShardGroup()
		go e.compact()
	})
}

func (e *Engine) Write(key Data, point *Point) error {
	// write key
	did := strconv.Itoa(int(point.DeviceId))
	if !e.keyDiskv.Has(did) {
		buffer := bytes.NewBuffer([]byte{})
		binary.Write(buffer, binary.BigEndian, key)
		err := e.keyDiskv.Write(did, buffer.Bytes())
		if err != nil {
			panic(err)
		}
	}

	// write data
	e.points <- point
	return nil
}

func (e *Engine) handleShardGroup() {
	for point := range e.points {
		// write value
		shardId := point.Timestamp / ShardSize

		// 		shardGroup: make(map[int64]chan *Point, 1e6),
		shard, ok := e.shardGroup.LoadOrStore(shardId, make(chan *Point, 1e6))
		if !ok {
			go e.handleShard(shardId, shard.(chan *Point))
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("chan had closed!")
					e.points <- point
				}
			}()
			shard.(chan *Point) <- point
		}()
	}
}

func (e *Engine) handleShard(shardId int64, points chan *Point) {
	list := NewSkipListMap[*Point, struct{}](&DataCompare{})
	size := 0
	for {
		select {
		case point, ok := <-points:
			if ok {
				list.Insert(point, struct{}{})
				size += len(point.Data)*8 + 16
				if size > 100*1e6 {
					go e.Dump(shardId, list)
					list = NewSkipListMap[*Point, struct{}](&DataCompare{})
				}
			} else {
				e.shardGroup.Delete(shardId)
				if list.Size() > 0 {
					go e.Dump(shardId, list)
				}
				goto Exit
			}
		case <-time.After(time.Second):
			close(points)
		}
	}
Exit:
	fmt.Println(shardId, "over")
}

func (e *Engine) dump(shardId int64, points chan *Point) {
	file, err := os.CreateTemp(TmpPath, fmt.Sprintf("%d-%d-", shardId, time.Now().Unix()))
	if err != nil {
		panic(err)
	}
	defer func() {
		file.Close()
		err := e.dataDiskv.Import(file.Name(), fmt.Sprintf("%d_%d_%d", ShardSize, shardId, time.Now().UnixMilli()), true)
		if err != nil {
			panic(err)
		}
		fmt.Println("import ok...")
	}()
	lastDid := -1
	start := int64(math.MaxInt64)
	end := int64(math.MinInt64)
	indexBuf := bytes.NewBuffer([]byte{})
	var offset uint64
	for k := range points {
		err = binary.Write(file, binary.BigEndian, k.Timestamp)
		if err != nil {
			panic(err)
		}
		err = binary.Write(file, binary.BigEndian, k.Data)
		if err != nil {
			panic(err)
		}
		if int(k.DeviceId) != lastDid && lastDid != -1 {
			var index = Index{
				DeviceId:  DeviceId(lastDid),
				StartTime: start,
				EndTime:   end,
				Offset:    int64(offset),
			}
			index.Write(indexBuf)
			start = math.MaxInt64
			end = math.MinInt64
		}
		offset += uint64(len(k.Data)*8) + 8
		if start > k.Timestamp {
			start = k.Timestamp
		}
		if end < k.Timestamp {
			end = k.Timestamp
		}
		lastDid = int(k.DeviceId)
	}
	if lastDid != -1 {
		var index = Index{
			DeviceId:  DeviceId(lastDid),
			StartTime: start,
			EndTime:   end,
			Offset:    int64(offset),
		}
		index.Write(indexBuf)
	}
	_, err = file.Write(indexBuf.Bytes())
	if err != nil {
		panic(err)
	}
	err = binary.Write(file, binary.BigEndian, int64(len(indexBuf.Bytes())))
	if err != nil {
		panic(err)
	}
	fmt.Println("write ok...")
}

func (e *Engine) Dump(shardId int64, list Skiplist[*Point, struct{}]) {
	fmt.Println("dump...", shardId, list.Size())
	iterator, err := list.Iterator()
	if err != nil {
		return
	}
	points := make(chan *Point, 1024)
	go e.dump(shardId, points)
	for {
		k, _, err := iterator.Next()
		if err != nil {
			break
		}
		points <- k
	}
	close(points)
	fmt.Println("close", shardId)
}

// data format

// [timestamp][value]... | [Index]... | [indexLength]

// [Index] = [device][start][end][offset][flag]

func (e *Engine) PrintAll(path string) []*Point {
	file, err := mmap.Open(path)
	if err != nil {
		panic(err)
	}
	indexLengthBuf := make([]byte, 8)
	file.ReadAt(indexLengthBuf, int64(file.Len())-8)
	var indexLength int64
	binary.Read(bytes.NewReader(indexLengthBuf), binary.BigEndian, &indexLength)
	fmt.Println("indexLength", indexLength, indexLengthBuf)

	// read index
	indexBuf := make([]byte, indexLength)
	file.ReadAt(indexBuf, int64(file.Len())-8-indexLength)
	reader := bytes.NewReader(indexBuf)
	for {
		index := Index{}
		err := index.Read(reader)
		if err != nil {
			break
		}
		fmt.Printf("%#v\n", index)

		keyBuffer, err := e.keyDiskv.Read(strconv.Itoa(int(index.DeviceId)))
		if err != nil {
			panic(err)
		}
		buf := make([]byte, 8+len(keyBuffer))

		file.ReadAt(buf, index.Offset)

		r := bytes.NewReader(buf)
		var timestamp, value int64
		var values Data
		binary.Read(r, binary.BigEndian, &timestamp)
		for i := 0; i < len(keyBuffer)/8; i++ {
			binary.Read(r, binary.BigEndian, &value)
			values = append(values, value)
		}
		fmt.Println(index.DeviceId, timestamp, values)
	}

	return nil
}
