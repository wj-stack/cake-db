package cake_db

import (
	"encoding/binary"
	"io"
)

type Data []int64

type Point struct {
	Data
	DeviceId
	Timestamp int64
}

type DeviceId uint32

const IndexSize = 4 + 8 + 8 + 8 + 1

type Index struct {
	DeviceId        // 4
	StartTime int64 // 8
	EndTime   int64 // 8
	Offset    int64 // 8
	Flag      byte  // 1
}

func (i *Index) Read(indexBuf io.Reader) error {
	err := binary.Read(indexBuf, binary.BigEndian, &i.DeviceId)
	if err != nil {
		return err
	}
	err = binary.Read(indexBuf, binary.BigEndian, &i.StartTime)
	if err != nil {
		return err
	}
	err = binary.Read(indexBuf, binary.BigEndian, &i.EndTime)
	if err != nil {
		return err
	}
	err = binary.Read(indexBuf, binary.BigEndian, &i.Offset)
	if err != nil {
		return err
	}
	err = binary.Read(indexBuf, binary.BigEndian, &i.Flag)
	if err != nil {
		return err
	}
	return nil
}
func (i Index) Write(indexBuf io.Writer) {
	binary.Write(indexBuf, binary.BigEndian, i.DeviceId)
	binary.Write(indexBuf, binary.BigEndian, i.StartTime)
	binary.Write(indexBuf, binary.BigEndian, i.EndTime)
	binary.Write(indexBuf, binary.BigEndian, i.Offset)
	binary.Write(indexBuf, binary.BigEndian, i.Flag)
}
