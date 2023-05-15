package dumpservice

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type File interface {
	io.ReaderAt
	io.ReadSeeker
}

type Service struct {
	solarDumpService SolarDumpService
	file             File
}

type SolarDumpService struct {
	Prefix string `gorm:"column:prefix;type:character varying(128);not null;default:''::character varying" json:"prefix"`
	Topic  string `gorm:"column:topic;type:character varying(64);not null;default:''::character varying" json:"topic"`
	Part   int32  `gorm:"column:part;type:integer;not null" json:"part"`
	Path   int32  `gorm:"column:path;type:integer;not null" json:"path"`
	File   int32  `gorm:"column:file;type:integer;not null" json:"file"`
	Off    int32  `gorm:"column:off;type:integer;not null" json:"off"`
	Cnt    int32  `gorm:"column:cnt;type:integer;not null" json:"cnt"`
}

func NewService(info SolarDumpService) *Service {
	return &Service{
		solarDumpService: info,
		file:             nil,
	}
}

func (service *Service) getFullPath() string {
	s := service.solarDumpService
	fullPath := fmt.Sprintf("%s/%s/%02d/%03d/%04d", s.Prefix, s.Topic, s.Part, s.Path, s.File)
	return fullPath
}

func (service *Service) getNextFile() string {
	s := service.solarDumpService
	f := s.File + 1
	p := s.Path
	if f >= 10000 {
		f = 0
		p++
	}
	fullPath := fmt.Sprintf("%s/%s/%02d/%03d/%04d", s.Prefix, s.Topic, s.Part, p, f)
	return fullPath
}

type Message struct {
	Len      int
	DeviceId int
	Offset   int
	Time     int
	Data     []byte
}

func (service *Service) FetchMessage() ([]Message, error) {
	if service.file == nil {
		path := service.getFullPath()
		file, err := os.Open(path)
		if err != nil {
			compressedData, err := os.Open(path + ".gz")
			if err != nil {
				return nil, err
			}
			gzReader, err := gzip.NewReader(compressedData)
			if err != nil {
				panic(err)
			}
			defer func(gzReader *gzip.Reader) {
				err := gzReader.Close()
				if err != nil {
					panic(err)
				}
			}(gzReader)

			buf, err := io.ReadAll(gzReader)
			if err != nil {
				return nil, err
			}
			service.file = bytes.NewReader(buf)

		} else {
			service.file = file
		}

		_, err = service.file.Seek(int64(service.solarDumpService.Off), io.SeekStart)
		if err != nil {
			return nil, err
		}
	}

	header := make([]byte, 24)

	ret := make([]Message, 0)

	for {

		// read header
		n, err := service.file.ReadAt(header, int64(service.solarDumpService.Off))

		if err != nil {
			// write is not finished, read next time
			// check new file

			_, err1 := os.Stat(service.getNextFile())
			_, err2 := os.Stat(service.getNextFile() + ".gz")

			// skip if a file already exists.
			if err1 != nil && err2 != nil {
				break
			}

			service.solarDumpService.Off = 0
			service.solarDumpService.Cnt = 0
			service.solarDumpService.File++
			if service.solarDumpService.File >= 10000 {
				service.solarDumpService.File = 0
				service.solarDumpService.Path++
			}
			service.file = nil
			break
		}

		msgLen := binary.BigEndian.Uint32(header[0:])
		msgDid := binary.BigEndian.Uint32(header[4:])
		msgOff := binary.BigEndian.Uint64(header[8:])
		msgTime := binary.BigEndian.Uint64(header[16:])

		data := make([]byte, msgLen)

		// read body
		n, err = service.file.ReadAt(data, int64(service.solarDumpService.Off)+24)
		if err != nil || n != int(msgLen) {
			break
		}

		ret = append(ret, Message{
			Len:      int(msgLen),
			DeviceId: int(msgDid),
			Offset:   int(msgOff),
			Time:     int(msgTime),
			Data:     data,
		})

		service.solarDumpService.Cnt++
		service.solarDumpService.Off += 24 + int32(msgLen)

	}
	return ret, nil
}

func (service *Service) LastCommit() SolarDumpService {
	s := service.solarDumpService
	return s
}
