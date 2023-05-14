package cake_db

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/go-mmap/mmap"
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

func (e *Engine) openIndexPipeline(files CompactFiles) chan *MergePoint {
	indexChan := make(chan *MergePoint, 1000)

	meta := strings.Split(files.key, "_")

	created, err := strconv.Atoi(meta[2])
	if err != nil {
		return nil
	}
	go func() {

		file, err := mmap.Open(files.path)
		if err != nil {
			panic(err)
		}
		indexLengthBuf := make([]byte, 8)
		file.ReadAt(indexLengthBuf, int64(file.Len())-8)
		var indexLength int64
		binary.Read(bytes.NewReader(indexLengthBuf), binary.BigEndian, &indexLength)

		// read index
		indexBuf := make([]byte, indexLength)
		file.ReadAt(indexBuf, int64(file.Len())-8-indexLength)
		reader := bufio.NewReader(bytes.NewReader(indexBuf))
		for {
			// read index
			index := Index{}
			err = index.Read(reader)
			if err != nil {
				break
			}

			// peek right
			var r int64
			peek, err := reader.Peek(IndexSize)
			if err != nil {
				r = int64(file.Len()) - 8 - indexLength
			} else {
				index := Index{}
				index.Read(bytes.NewReader(peek))
				r = index.Offset
			}
			fmt.Printf("%#v\n", index)
			//fmt.Println(index.Offset, r)
			// read key
			keyBuffer, err := e.keyDiskv.Read(strconv.Itoa(int(index.DeviceId)))
			if err != nil {
				panic(err)
			}

			// read data
			buf := make([]byte, 8+len(keyBuffer))

			for i := index.Offset; i < r; i += int64(8 + len(keyBuffer)) {
				file.ReadAt(buf, i)
				r := bytes.NewReader(buf)
				var timestamp, value int64
				var values Data
				binary.Read(r, binary.BigEndian, &timestamp)
				for i := 0; i < len(keyBuffer)/8; i++ {
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
	key  string
	path string
	size int64
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
			if stat.Size() < 1*1e9 {
				v[split[1]] = append(v[split[1]], CompactFiles{
					key:  key,
					path: path,
					size: stat.Size(),
				})
			}
		}

		for shardId, files := range v {
			sort.Slice(files, func(i, j int) bool {
				return files[i].size < files[j].size
			})
			if len(files) <= 5 {
				continue
			}
			atoi, err := strconv.Atoi(shardId)
			if err != nil {
				panic(err)
			}
			e.merge(int64(atoi), files)
		}
		time.Sleep(time.Minute * 5)
	}
}

func (e *Engine) merge(shardId int64, files []CompactFiles) {
	var c []chan *MergePoint
	for _, i := range files {
		pipeline := e.openIndexPipeline(i)
		c = append(c, pipeline)
	}
	points := MergeN(c...)
	lastDid := -1
	lastTimestamp := -1
	target := make(chan *Point, 1000)
	go e.dump(shardId, target)
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
		e.dataDiskv.Erase(i.key)
	}
}
