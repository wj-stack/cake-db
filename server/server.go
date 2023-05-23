package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/duke-git/lancet/v2/convertor"
	"github.com/go-mmap/mmap"
	"github.com/juju/errors"
	"github.com/peterbourgon/diskv/v3"
	"github.com/pierrec/lz4"
	"io"
	"math"
	"os"
	"strings"
	"sync"
)

type Server interface {
	CreateDB(name string) error
}

type DB interface {
	WriteValue(deviceId, timestamp int64, values []int64, id uint64) error
	WriteKey(deviceId int64, key []int64) error
	Id() uint64
	ReadValue(deviceId, start, end int64) ([]int64, error)
	ReadKey(deviceId int64) ([]int64, error)
}

type Data struct {
	DeviceId  int64
	Timestamp int64
	Value     []int64
	Id        uint64
}

type Cake struct {
	key      *diskv.Diskv
	value    *diskv.Diskv
	maxId    uint64
	commitId uint64
	idMu     sync.RWMutex
	shardMu  sync.RWMutex
	shard    Skiplist[*Data, struct{}]
	size     int
}

func NewCake(basePath string) *Cake {
	return &Cake{
		key: diskv.New(diskv.Options{
			BasePath: basePath + "/key",
			Transform: func(s string) []string {
				if len(s) > 2 {
					return []string{s[:2]}
				}
				return []string{"0" + s[:1]}
			},
			CacheSizeMax: 200 * 1e6,
			TempDir:      basePath + "/tmp",
		}),
		value: diskv.New(diskv.Options{
			BasePath: basePath + "/value",
			Transform: func(s string) []string {
				split := strings.Split(s, "_")
				return split[:2]
			},
			CacheSizeMax: 0, // use mmap
			TempDir:      basePath + "/tmp",
		}),
		shard: NewSkipListMap[*Data, struct{}](&DataCompare{}),
	}
}

func (db *Cake) WriteValue(deviceId, timestamp int64, values []int64, id uint64) error {

	// check id
	db.idMu.RUnlock()
	if id <= db.maxId {
		return errors.New("out of order")
	}
	db.idMu.RUnlock()

	// modify max id
	db.idMu.Lock()
	db.maxId = id
	db.idMu.Unlock()

	// insert
	db.shardMu.Lock()
	defer db.shardMu.Unlock()

	db.shard.Insert(&Data{
		DeviceId:  deviceId,
		Timestamp: timestamp,
		Value:     values,
		Id:        id,
	}, struct{}{})
	db.size += 8 * (len(values) + 1)

	if db.size > 1e6*100 {
		db.size = 0
		// dump
	}

	return nil
}

func getWriter(zip bool) (io.Writer, *bytes.Buffer) {
	var writer io.Writer
	buffer := bytes.NewBuffer([]byte{})
	if zip {
		writer = lz4.NewWriter(buffer)
	} else {
		writer = buffer
	}
	return writer, buffer
}

func (e *Cake) dump(shardSize, shardId int64, created int64, points chan *Data, zip bool) {
	// create a tmp file
	file, err := os.CreateTemp("/tmp/", fmt.Sprintf("%d-%d-", shardId, created))
	if err != nil {
		panic(err)
	}
	lastDid := -1
	start := int64(math.MaxInt64)
	end := int64(math.MinInt64)
	indexBuf := bytes.NewBuffer([]byte{})
	var offset uint64
	var writer io.Writer
	var reader *bytes.Buffer
	writer, reader = getWriter(zip)
	var flag byte
	if zip {
		flag &= 1
	}
	realSize := 0

	writeIndex := func() {
		closer, ok := writer.(io.Closer)
		if ok {
			_ = closer.Close()
		}
		n, err := io.Copy(file, reader)
		if err != nil {
			panic(err)
		}
		var index = Index{
			DeviceId:  uint32(lastDid),
			StartTime: start,
			EndTime:   end,
			Offset:    int64(offset),
			Length:    int64(realSize),
			Flag:      flag,
		}
		index.Write(indexBuf)
		start = math.MaxInt64
		end = math.MinInt64
		offset += uint64(n)
		realSize = 0
		writer, reader = getWriter(zip)
	}

	writeData := func(k *Data) {
		p := bytes.NewBuffer([]byte{})
		err = binary.Write(p, binary.BigEndian, k.Timestamp)
		if err != nil {
			panic(err)
		}
		err = binary.Write(p, binary.BigEndian, k.Value)
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
	}

	for k := range points {
		if int(k.DeviceId) != lastDid {
			if lastDid != -1 {
				writeIndex()
			}
			writeData(k)
		}
		lastDid = int(k.DeviceId)
	}
	writeIndex()

	// write index
	_, err = file.Write(indexBuf.Bytes())
	if err != nil {
		panic(err)
	}

	// write index length
	err = binary.Write(file, binary.BigEndian, int64(len(indexBuf.Bytes())))
	if err != nil {
		panic(err)
	}

	// close and import file
	err = file.Close()
	if err != nil {
		panic(err)
	}

	name := fmt.Sprintf("%d_%d_%d", shardSize, shardId, created)
	err = e.value.Import(file.Name(), name, true)
	if err != nil {
		panic(err)
	}
}

func getValuePath(prefix, s string) string {
	split := strings.Split(s, "_")
	str := prefix + "/"
	for _, i := range split[:2] {
		str += i + "/"
	}
	str += s
	return str
}
func (db *Cake) readAll(key string) error {
	path := getValuePath(db.value.BasePath, key)
	file, err := mmap.Open(path)
	if err != nil {
		return err
	}

	indexLength, err := readIndexLength(file)
	if err != nil {
		return err
	}

	allIndex, err := readAllIndex(file, indexLength)
	if err != nil {
		return err
	}

}

func (db *Cake) WriteKey(deviceId int64, key []int64) error {
	encodeByte, err := convertor.EncodeByte(key)
	if err != nil {
		return err
	}
	return db.key.Write(convertor.ToString(deviceId), encodeByte)
}

func (db *Cake) ReadValue(deviceId, start, end int64) ([]int64, error) {
	return nil, nil
}

func (db *Cake) ReadKey(deviceId int64) ([]int64, error) {
	bytes, err := db.key.Read(convertor.ToString(deviceId))
	if err != nil {
		return nil, err
	}
	var v []int64
	err = convertor.DecodeByte(bytes, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (db *Cake) Id() uint64 {
	return db.commitId
}
