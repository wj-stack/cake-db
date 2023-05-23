package server

import (
	cakedb "cake-db"
	"fmt"
	"testing"
	"time"
)

func TestCake_WriteKey(t *testing.T) {
	db := NewCake("/home/wyatt/cake-db/deltaSolar")
	err := db.WriteKey(1234, []int64{1, 2, 3, 4, 5})
	if err != nil {
		panic(err)
	}
}

func TestCake_ReadKey(t *testing.T) {
	db := NewCake("/home/wyatt/cake-db/deltaSolar")
	key, err := db.ReadKey(1234)
	if err != nil {
		panic(err)
	}
	fmt.Println(key)
}

func TestCake_WriteValue(t *testing.T) {
	db := NewCake("/home/wyatt/cake-db/deltaSolar")
	for i := 0; i < 1e6; i++ {
		err := db.WriteValue(1, 1, nil, uint64(i+1))
		if err != nil {
			panic(err)
		}
	}
}

func TestCake_dump(t *testing.T) {
	db := NewCake("/home/wyatt/cake-db/deltaSolar")
	data := make(chan *Data)
	go db.dump(0, 0, 0, data, false)
	for i := 0; i < 100; i++ {
		data <- &Data{
			DeviceId:  int64(i),
			Timestamp: int64(i),
			Value:     []int64{int64(i)},
			Id:        uint64(i + 1),
		}
	}
	close(data)
	time.Sleep(time.Second * 10)
}

func TestCake_dump2(t *testing.T) {
	db := NewCake("/home/wyatt/cake-db/deltaSolar")
	data := make(chan *Data)
	go db.dump(0, 0, 1, data, true)
	for i := 0; i < 100; i++ {
		data <- &Data{
			DeviceId:  int64(i),
			Timestamp: int64(i),
			Value:     []int64{int64(i)},
			Id:        uint64(i + 1),
		}
	}
	close(data)
	time.Sleep(time.Second * 10)
}
func TestCake_read1(t *testing.T) {
	//db := NewCake("/home/wyatt/cake-db/deltaSolar")
	engine := &cakedb.Engine{}
	pipeline := engine.OpenIndexPipeline(cakedb.CompactFiles{
		Key:  "0_0_0",
		Path: "/home/wyatt/cake-db/deltaSolar/value/0/0/0_0_0",
		Size: 0,
	})
	for i := range pipeline {
		fmt.Println(i.DeviceId, i.Timestamp, i.Data, i.Created)
	}
}
