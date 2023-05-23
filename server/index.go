package server

import (
	"encoding/binary"
	"io"
)

const IndexSize = 4 + 8 + 8 + 8 + 8 + 1

type Index struct {
	DeviceId  uint32 // 4
	StartTime int64  // 8
	EndTime   int64  // 8
	Offset    int64  // 8
	Length    int64  // 8  if it is compressed, it is the original size
	Flag      byte   // 1
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
	err = binary.Read(indexBuf, binary.BigEndian, &i.Length)
	if err != nil {
		return err
	}
	err = binary.Read(indexBuf, binary.BigEndian, &i.Flag)
	if err != nil {
		return err
	}
	return nil
}
func (i *Index) Write(indexBuf io.Writer) {
	binary.Write(indexBuf, binary.BigEndian, i.DeviceId)
	binary.Write(indexBuf, binary.BigEndian, i.StartTime)
	binary.Write(indexBuf, binary.BigEndian, i.EndTime)
	binary.Write(indexBuf, binary.BigEndian, i.Offset)
	binary.Write(indexBuf, binary.BigEndian, i.Length)
	binary.Write(indexBuf, binary.BigEndian, i.Flag)
}
