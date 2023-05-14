package main

import (
	cake_db "cake-iot"
	"math/rand"
	"time"
)

func main() {
	engine := cake_db.New()
	engine.Init()
	// mock 50000 device ， 120 register
	key := make([]int64, 120*8)
	data := make([]int64, 120*8)
	t := time.Now().UnixNano()
	for i := 0; i < 1000000; i++ {
		engine.Write(key, &cake_db.Point{
			Data:      data,
			DeviceId:  cake_db.DeviceId(rand.Int() % 20000),
			Timestamp: int64(t + int64(rand.Int()%200000)),
		})
	}
	time.Sleep(time.Second * 10)
}
