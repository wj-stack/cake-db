package main

import (
	cake_db "cake-iot"
	"cake-iot/pkg/analyzer"
	dumpservice "cake-iot/pkg/dumperservice"
	"fmt"
	"time"
)

func Lz4() {

}

func main() {
	engine := cake_db.New()
	engine.Init()

	service := dumpservice.NewService(dumpservice.SolarDumpService{
		Prefix: "/data/dumpdb/delta-solar",
		Topic:  "delta-solar",
		Part:   0,
		Path:   0,
		File:   20,
		Off:    0,
		Cnt:    0,
	})

	solarAnalyzer := analyzer.SolarAnalyzer{}
	for {
		message, err := service.FetchMessage()
		if err != nil {
			panic(err)
		}
		if len(message) == 0 {
			continue
		}
		for _, msg := range message {
			analyze, err := solarAnalyzer.Analyze(analyzer.Message{
				Offset:    msg.Offset,
				CreatedAt: time.UnixMilli(int64(msg.Time)),
				DeviceId:  msg.DeviceId,
				Bytes:     msg.Data,
				Length:    msg.Len,
			})
			if err != nil {
				continue
			}
			for _, i := range analyze {
				if i.MsgType == 80 {
					m := i.Data.(analyzer.SolarMessage)
					if m.GetSolarData() == nil {
						continue
					}
					if len(m.GetSolarData().Regs) > 0 {
						regs := m.GetSolarData().Regs
						var key []int64
						var value []int64
						for _, reg := range regs {
							key = append(key, int64(reg[0]))
							value = append(value, int64(reg[1]))
						}
						engine.Write(key, &cake_db.Point{
							Data:      value,
							DeviceId:  cake_db.DeviceId(i.DeviceId),
							Timestamp: i.UpdatedAt.UnixNano(),
						})
					}

				}
			}
		}

		fmt.Println(time.Now(), service.LastCommit())
	}

}
