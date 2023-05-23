package cakedb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/go-mmap/mmap"
	"github.com/pierrec/lz4"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type MergePoint struct {
	*Point
	Created int64
}

func (e *Engine) OpenIndexPipeline(files CompactFiles) chan *MergePoint {
	indexChan := make(chan *MergePoint, 1000)

	meta := strings.Split(files.Key, "_")

	created, err := strconv.Atoi(meta[2])
	if err != nil {
		return nil
	}
	go func() {

		file, err := mmap.Open(files.Path)
		if err != nil {
			panic(err)
		}
		indexLengthBuf := make([]byte, 8)
		file.ReadAt(indexLengthBuf, int64(file.Len())-8)
		var indexLength int64
		binary.Read(bytes.NewReader(indexLengthBuf), binary.BigEndian, &indexLength)

		// read all index
		indexBuf := make([]byte, indexLength)
		file.ReadAt(indexBuf, int64(file.Len())-8-indexLength)
		reader := bufio.NewReader(bytes.NewReader(indexBuf))

		// read all index
		// TODO: chang to binray search
		var indexs []*Index
		for {
			index := Index{}
			err = index.Read(reader)
			if err != nil {
				break
			}
			indexs = append(indexs, &index)
			fmt.Println(index)
		}

		//fmt.Println("indexs:", len(indexs))

		dataSize := int64(file.Len()) - 8 - indexLength
		for i := 0; i < len(indexs); i++ {
			l := indexs[i].Offset
			r := dataSize
			if i != len(indexs)-1 {
				r = indexs[i+1].Offset
			}

			index := indexs[i]

			// read Key
			keyBuffer, err := e.keyDiskv.Read(strconv.Itoa(int(index.DeviceId)))
			if err != nil {
				panic(err)
			}
			//fmt.Println("l", l, "r", r)

			valueCount := len(keyBuffer) / 8
			pointSize := int64(8 + valueCount*8)

			buf := make([]byte, r-l)
			file.ReadAt(buf, index.Offset)
			if index.Flag&1 > 0 {
				//fmt.Println("unzip:", index, "len:", index.Length, "zip length:", r-l, files.Key, buf)
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

		}
		close(indexChan)
	}()
	return indexChan
}

func cmpIndexAndKey(a, b *MergePoint) bool {
	if a.DeviceId != b.DeviceId {
		return a.DeviceId < b.DeviceId
	}
	if a.Timestamp != b.Timestamp {
		return a.Timestamp < b.Timestamp
	}
	return a.Created < b.Created
}

func merge(in1, in2 chan *MergePoint) chan *MergePoint {
	out := make(chan *MergePoint, 1000)
	go func() {
		v1, ok1 := <-in1
		v2, ok2 := <-in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && cmpIndexAndKey(v1, v2)) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		close(out)
	}()
	return out
}

func MergeN(inputs ...chan *MergePoint) chan *MergePoint {
	if len(inputs) == 1 {
		return inputs[0]
	}
	middle := len(inputs) / 2
	return merge(MergeN(inputs[:middle]...), MergeN(inputs[middle:]...))
}

type CompactFiles struct {
	Key  string
	Path string
	Size int64
}

func (e *Engine) compact() {
	for {
		v := map[string][]CompactFiles{}
		keys := e.dataDiskv.Keys(nil)
		for key := range keys {
			path := GetValuePath(key)
			stat, err := os.Stat(path)
			if err != nil {
				continue
			}
			split := strings.Split(key, "_")
			if stat.Size() < 500*1e6 {
				v[split[1]] = append(v[split[1]], CompactFiles{
					Key:  key,
					Path: path,
					Size: stat.Size(),
				})
			}
		}

		for shardId, files := range v {
			sort.Slice(files, func(i, j int) bool {
				return files[i].Size < files[j].Size
			})
			if len(files) <= 5 {
				continue
			}

			size := int64(0)
			for _, i := range files {
				size += i.Size
			}

			atoi, err := strconv.Atoi(shardId)
			if err != nil {
				panic(err)
			}

			op := DumpOptional{}
			if size > 100*1e6 {
				op.Zip = true
			}

			fmt.Println(time.Now(), "start compact", files)
			e.merge(int64(atoi), files, &op)
			fmt.Println(time.Now(), "end compact", files)

		}
		time.Sleep(time.Minute)
	}
}

type DumpOptional struct {
	Zip bool
}

func (e *Engine) merge(shardId int64, files []CompactFiles, op *DumpOptional) {
	var c []chan *MergePoint
	for _, i := range files {
		pipeline := e.OpenIndexPipeline(i)
		c = append(c, pipeline)
	}
	points := MergeN(c...)
	lastDid := -1
	lastTimestamp := -1
	target := make(chan *Point, 1000)
	go e.dump(shardId, target, op)
	for i := range points {
		if int(i.DeviceId) != lastDid || lastTimestamp != int(i.Timestamp) {
			target <- &Point{
				Data:      i.Data,
				DeviceId:  i.DeviceId,
				Timestamp: i.Timestamp,
			}
		}
		lastTimestamp = int(i.Timestamp)
		lastDid = int(i.DeviceId)
	}
	close(target)
	for _, i := range files {
		e.dataDiskv.Erase(i.Key)
	}
}
