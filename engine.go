package cakedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/go-mmap/mmap"
	"github.com/peterbourgon/diskv/v3"
	"github.com/pierrec/lz4"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const ShardSize = int64(time.Hour * 7 * 24)

type Engine struct {
	points              chan *Point
	shardGroup          sync.Map
	mu                  sync.RWMutex
	list                Skiplist[*Point, struct{}]
	keyDiskv, dataDiskv *diskv.Diskv
}

//const KeyPath = "/home/wyatt/code/tmp/data/Key"
//const ValuePath = "/home/wyatt/code/tmp/data/value"
//const TmpPath = "/home/wyatt/code/tmp/data/tmp"

const KeyPath = "/data/cake-db/data/Key"
const ValuePath = "/data/cake-db/data/value"
const TmpPath = "/data/cake-db//data/tmp"

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
	os.MkdirAll(KeyPath, 0777)
	os.MkdirAll(ValuePath, 0777)
	os.MkdirAll(TmpPath, 0777)
	return &Engine{
		points:    make(chan *Point, 1e6),
		list:      NewSkipListMap[*Point, struct{}](&DataCompare{}),
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
	// write Key
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
	size := 0
	for point := range e.points {
		e.mu.Lock()
		e.list.Insert(point, struct{}{})
		e.mu.Unlock()
		size += len(point.Data)*8 + 16
		if size > 100*1e6 {
			size = 0
			go func(list Skiplist[*Point, struct{}]) {
				c := map[int64]chan *Point{}
				iterator, err := list.Iterator()
				if err != nil {
					panic(err)
				}
				wg := sync.WaitGroup{}
				for {
					k, _, err := iterator.Next()
					if err != nil {
						break
					}
					shardId := k.Timestamp / ShardSize
					_, ok := c[shardId]
					if !ok {
						c[shardId] = make(chan *Point, 1e6)
						wg.Add(1)
						go func() {
							e.dump(shardId, c[shardId], nil)
							wg.Done()
						}()
					}
					c[shardId] <- k
				}
				for _, c := range c {
					close(c)
				}
				wg.Wait()
			}(e.list)
			// clear
			e.mu.Lock()
			e.list = NewSkipListMap[*Point, struct{}](&DataCompare{})
			e.mu.Unlock()
		}
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

func getWriter(op *DumpOptional) (io.Writer, *bytes.Buffer) {
	var writer io.Writer
	buffer := bytes.NewBuffer([]byte{})
	if op != nil && op.Zip {
		writer = lz4.NewWriter(buffer)
	} else {
		writer = buffer
	}
	return writer, buffer
}

func (e *Engine) dump(shardId int64, points chan *Point, op *DumpOptional) {
	file, err := os.CreateTemp(TmpPath, fmt.Sprintf("%d-%d-", shardId, time.Now().Unix()))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := recover(); err != nil {
			file.Close()
			os.Remove(file.Name())
			return
		}
		file.Close()
		name := fmt.Sprintf("%d_%d_%d", ShardSize, shardId, time.Now().UnixMilli())

		err := e.dataDiskv.Import(file.Name(), name, true)
		if err != nil {
			panic(err)
		}
		fmt.Println("import ok...", name)
		if op != nil && op.Zip {
			fmt.Println("zip ok ...", name)
		}

	}()
	lastDid := -1
	start := int64(math.MaxInt64)
	end := int64(math.MinInt64)
	indexBuf := bytes.NewBuffer([]byte{})
	var offset uint64
	var writer io.Writer
	var reader *bytes.Buffer
	var n int64
	var flag byte
	if op != nil && op.Zip {
		flag = 1
	}
	realSize := 0
	for k := range points {

		if int(k.DeviceId) != lastDid {
			if lastDid != -1 {
				closer, ok := writer.(io.Closer)
				if ok {
					closer.Close()
				}
				n, err = io.Copy(file, reader)
				//if op != nil && op.Zip {
				//	fmt.Println("unzip", realSize, "zip:", n)
				//}
				if err != nil {
					panic(err)
				}
				var index = Index{
					DeviceId:  DeviceId(lastDid),
					StartTime: start,
					EndTime:   end,
					Offset:    int64(offset),
					Length:    int64(realSize),
					Flag:      flag,
				}
				//fmt.Println("offset", int64(offset))
				index.Write(indexBuf)
				start = math.MaxInt64
				end = math.MinInt64
				offset += uint64(n)
			}
			realSize = 0
			writer, reader = getWriter(op)
		}

		p := bytes.NewBuffer([]byte{})
		err = binary.Write(p, binary.BigEndian, k.Timestamp)
		if err != nil {
			panic(err)
		}
		err = binary.Write(p, binary.BigEndian, k.Data)
		if err != nil {
			panic(err)
		}

		realSize += p.Len()
		_, err := writer.Write(p.Bytes())
		if err != nil {
			panic(err)
		}

		if start > k.Timestamp {
			start = k.Timestamp
		}
		if end < k.Timestamp {
			end = k.Timestamp
		}

		lastDid = int(k.DeviceId)
	}

	if lastDid != -1 {
		closer, ok := writer.(io.Closer)
		if ok {
			closer.Close()
		}
		n, err = io.Copy(file, reader)
		if err != nil {
			panic(err)
		}
		var index = Index{
			DeviceId:  DeviceId(lastDid),
			StartTime: start,
			EndTime:   end,
			Offset:    int64(offset),
			Length:    int64(realSize),
			Flag:      flag,
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
	go e.dump(shardId, points, nil)
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

func (e *Engine) Read(did DeviceId, start, end int64) (RetKey Data, value []Point, err error) {

	keyBuf, err := e.keyDiskv.Read(strconv.Itoa(int(did)))
	if err != nil {
		return nil, nil, err
	}
	keyReader := bytes.NewReader(keyBuf)
	RetKey = make(Data, len(keyBuf)/8)
	err = binary.Read(keyReader, binary.BigEndian, RetKey)
	if err != nil {
		return nil, nil, err
	}
	v := map[int64]*MergePoint{}
	mu := sync.Mutex{}
	startId := start / ShardSize
	endId := end / ShardSize
	keys := e.dataDiskv.Keys(nil)
	wp := sync.WaitGroup{}
	for key := range keys {
		fmt.Println("RetKey:", key)
		path := GetValuePath(key)
		split := strings.Split(key, "_")
		atoi, err := strconv.Atoi(split[1])
		fmt.Println(atoi, startId, endId)
		if err != nil {
			return nil, nil, err
		}
		if atoi < int(startId) || atoi > int(endId) {
			continue
		}
		fmt.Println("read:", key, "path:", path)
		wp.Add(1)
		go func() {
			defer wp.Done()
			stream, err := e.dataDiskv.ReadStream(key, true)
			if err != nil {
				return
			}
			defer stream.Close()
			stat, err := os.Stat(path)
			if err != nil {
				fmt.Println(err)
				return
			}
			points := e.read(CompactFiles{
				Key:  key,
				Path: path,
				Size: stat.Size(),
			}, RetKey, did, start, end)

			for msg := range points {
				mu.Lock()
				if v[msg.Timestamp] == nil || v[msg.Timestamp].Created < msg.Created {
					v[msg.Timestamp] = msg
				}
				mu.Unlock()
			}
		}()
	}
	wp.Wait()

	for _, i := range v {
		value = append(value, Point{
			Data:      i.Data,
			DeviceId:  i.DeviceId,
			Timestamp: i.Timestamp,
		})
	}
	return RetKey, value, nil
}

func (e *Engine) read(files CompactFiles, key Data, did DeviceId, start, end int64) chan *MergePoint {
	indexChan := make(chan *MergePoint, 1000)

	meta := strings.Split(files.Key, "_")

	created, err := strconv.Atoi(meta[2])
	if err != nil {
		return nil
	}

	go func() {
		defer close(indexChan)
		file, err := mmap.Open(files.Path)
		if err != nil {
			panic(err)
		}

		// read Index length
		indexLengthBuf := make([]byte, 8)
		file.ReadAt(indexLengthBuf, int64(file.Len())-8)
		var indexLength int64
		binary.Read(bytes.NewReader(indexLengthBuf), binary.BigEndian, &indexLength)

		n := indexLength / IndexSize

		fmt.Println("indexLength:", indexLength, "n:", n)

		indexBuf := make([]byte, IndexSize)

		index := Index{}
		search := sort.Search(int(n), func(i int) bool {
			_, err := file.ReadAt(indexBuf, int64(file.Len())-8-indexLength+int64(i*IndexSize))
			if err != nil {
				panic(err)
			}
			reader := bytes.NewReader(indexBuf)
			index.Read(reader)
			//fmt.Printf("index:%#v\n", index)
			return index.DeviceId >= did
		})

		if index.DeviceId != did {
			return
		}

		fmt.Printf("index:%#v\n", index)

		l := index.Offset
		r := int64(file.Len()) - 8 - indexLength
		if int64(search) != n-1 {
			nextIndex := Index{}
			_, err := file.ReadAt(indexBuf, int64(file.Len())-8-indexLength+int64((search+1)*IndexSize))
			if err != nil {
				panic(err)
			}
			reader := bytes.NewReader(indexBuf)
			nextIndex.Read(reader)
			r = nextIndex.Offset
		}

		valueCount := len(key)
		pointSize := int64(8 + valueCount*8)

		buf := make([]byte, r-l)
		file.ReadAt(buf, index.Offset)
		if index.Flag&1 > 0 {
			target := make([]byte, index.Length)
			_, err := lz4.NewReader(bytes.NewReader(buf)).Read(target)
			if err != nil {
				panic(err)
			}
			buf = target
		} else {
			file.ReadAt(buf, index.Offset)
		}

		for i := 0; i < int(index.Length/pointSize); i++ {
			r := bytes.NewReader(buf[i*int(pointSize) : (i+1)*int(pointSize)])
			var timestamp, value int64
			var values Data
			binary.Read(r, binary.BigEndian, &timestamp)
			for i := 0; i < valueCount; i++ {
				binary.Read(r, binary.BigEndian, &value)
				values = append(values, value)
			}
			v := &MergePoint{
				Point: &Point{
					Data:      values,
					DeviceId:  index.DeviceId,
					Timestamp: timestamp,
				},
				Created: int64(created),
			}
			//fmt.Println(v.DeviceId, v.Timestamp, v.Data)
			indexChan <- v
		}

	}()
	return indexChan
}

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
		fmt.Println("did", index.DeviceId, "timestamp", timestamp, "value:", values, "len:", len(values))
	}

	return nil
}
