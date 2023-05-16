package main

import (
	cake_db "cake-iot"
	"fmt"
	"time"
)

func main() {
	TestRead()
}
func TestRead() {
	engine := cake_db.New()
	engine.Init()

	pipeline := engine.OpenIndexPipeline(cake_db.CompactFiles{
		Key:  "604800000000000_2757_1684221366334",
		Path: "/data/cake-db/data/value/604800000000000/2757/604800000000000_2757_1684221366334",
		Size: 0,
	})
	cnt := 0
	t := time.Now()
	for _ = range pipeline {
		//fmt.Println(msg.DeviceId, msg.Timestamp, msg.Data)
		cnt++
	}
	fmt.Println("cnt:", cnt, time.Now().UnixMilli()-t.UnixMilli())
}
