package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/go-mmap/mmap"
	"github.com/pierrec/lz4"
)

func readIndexLength(file *mmap.File) (int64, error) {
	indexLengthBuf := make([]byte, 8)
	_, err := file.ReadAt(indexLengthBuf, int64(file.Len())-8)
	if err != nil {
		return 0, err
	}
	var indexLength int64
	err = binary.Read(bytes.NewReader(indexLengthBuf), binary.BigEndian, &indexLength)
	if err != nil {
		return 0, err
	}
	return indexLength, nil
}

func readAllIndex(file *mmap.File, indexLength int64) ([]*Index, error) {
	indexBuf := make([]byte, indexLength)
	_, err := file.ReadAt(indexBuf, int64(file.Len())-8-indexLength)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(bytes.NewReader(indexBuf))
	var v []*Index
	for {
		i := Index{}
		err := i.Read(reader)
		if err != nil {
			break
		}
		v = append(v, &i)
	}
	return v, nil
}

func readDataByIndex(file *mmap.File, index Index, valueCount int, size int) ([]int64, [][]int64, error) {
	pointSize := int64(8 + valueCount*8)
	buf := make([]byte, size)
	_, err := file.ReadAt(buf, index.Offset)
	if err != nil {
		return nil, nil, err
	}

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

}
