package cakedb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/dlclark/regexp2"
	"github.com/spf13/afero"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEngine_Write(t *testing.T) {
	engine := New()
	engine.Init()

	for i := 0; i < 1e6; i++ {
		engine.Write([]int64{1, 2, 3}, &Point{
			Data:      []int64{int64(i), int64(i), int64(i)},
			DeviceId:  DeviceId(i) % 3,
			Timestamp: int64(i),
		})
	}
	time.Sleep(time.Second * 5)
}

func TestEngine_PrintAll(t *testing.T) {
	engine := New()
	engine.Init()
	engine.PrintAll("/home/wyatt/code/cake-db/0-1683988876-4293721648")
}

func TestEngine_Compact(t *testing.T) {
	engine := New()
	engine.Init()
	time.Sleep(time.Second * 10)
}

func TestEngine_Channle(t *testing.T) {
	c := make(chan int, 100)
	go func() {
		time.Sleep(time.Second)
		close(c)
	}()
	for i := range c {
		time.Sleep(time.Second)
		fmt.Println(i)
	}
	fmt.Println("close")

}

func TestEngine_InfluxTest(t *testing.T) {
	file, err := os.ReadFile("influx.log")
	if err != nil {
		return
	}
	n := len("2023-04-23T05:27:50Z")
	reader := bufio.NewReader(bytes.NewReader(file))
	mapping := map[int]time.Time{}
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		str := string(line)

		r := regexp2.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\s+\[INFO\]\s+solar_pgsql\s+Fetch\stime:\s+(\d+)ms\s+full\(([\w\/\.\-]+)\)\s+count\(([\d\/]+)\)\s+off\((\d+)\)`, 0)
		matchs, err := r.FindStringMatch(str)
		if err != nil {
			panic(err)
		}

		if matchs != nil {
			//for _, i := range matchs.Groups() {
			//	fmt.Println(i.Name, i.String())
			//}
			split := strings.Split(matchs.Groups()[2].String(), "/")
			//fmt.Println(str[:n], split[7])
			atoi, err := strconv.Atoi(split[7])
			if err != nil {
				panic(err)
			}
			mapping[atoi] = dateparse.MustParse(str[:n])
		}

	}

	tt := 0
	s := ""
	for i := 27; i < 4022; i++ {
		if mapping[i+1].Unix()-mapping[i].Unix() > 0 && mapping[i+1].Unix()-mapping[i].Unix() < 1000 {
			tt += int(mapping[i+1].Unix() - mapping[i].Unix())
		}
		s += strconv.Itoa(tt) + ","
	}
	fmt.Println(s)
}

func TestEngine_EngineTest(t *testing.T) {
	file, err := os.ReadFile("iot-db.log")
	if err != nil {
		return
	}
	n := len(`time="2023-04-23T23:20:19Z"`)
	reader := bufio.NewReader(bytes.NewReader(file))
	mapping := map[int]time.Time{}
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		str := string(line)

		if strings.Contains(str, "message") {
			between, err := getTextBetween(str, "message: 100000 ", " 0 2023")
			if err != nil {
				panic(err)
			}
			atoi, err := strconv.Atoi(between)
			if err != nil {
				panic(err)
			}
			mapping[atoi] = dateparse.MustParse(str[6 : n-1])
		}

	}
	tt := 0
	s := ""
	for i := 27; i < 4022; i++ {
		if mapping[i+1].Unix()-mapping[i].Unix() > 0 {
			tt += int(mapping[i+1].Unix() - mapping[i].Unix())
		}
		s += strconv.Itoa(tt) + ","
	}
	fmt.Println(s)

}
func getTextBetween(str, start, end string) (string, error) {
	startIndex := strings.Index(str, start)
	if startIndex == -1 {
		return "", fmt.Errorf("start string not found")
	}

	endIndex := strings.Index(str[startIndex+len(start):], end)
	if endIndex == -1 {
		return "", fmt.Errorf("end string not found")
	}

	return str[startIndex+len(start) : startIndex+len(start)+endIndex], nil
}

func TestEngine_GenJson(t *testing.T) {
	rand.Seed(20230423)
	v := map[uint16]int64{}
	for i := 0; i < 120; i++ {
		v[uint16(rand.Int()%65535)] = int64(rand.Int())
	}
	marshal, err := json.Marshal(v)
	if err != nil {
		return
	}

	fmt.Println("json:", len(marshal), "binary:", len(v)*10, "protobuf:")

}

func TestLz4Write(t *testing.T) {
	engine := New()
	engine.Init()
	points := make(chan *Point, 1e4)
	go engine.dump(-1, points, &DumpOptional{Zip: true})
	for i := 0; i < 100; i++ { // 100个设备
		for k := 0; k < 1000; k++ { // 1000条数据
			var v []int64
			for j := 0; j < 10; j++ { // 10个寄存器
				v = append(v, int64(i))
			}
			points <- &Point{
				Data:      v,
				DeviceId:  DeviceId(i),
				Timestamp: int64(k),
			}
		}

	}
	close(points)
	fmt.Println("write ok...")
	time.Sleep(time.Second * 10)
}

func TestLz4WriteNoLz4(t *testing.T) {
	engine := New()
	engine.Init()
	points := make(chan *Point, 1e4)
	go engine.dump(-1, points, &DumpOptional{Zip: false})
	for i := 0; i < 100; i++ { // 100个设备
		for k := 0; k < 1000; k++ { // 1000条数据
			var v []int64
			for j := 0; j < 10; j++ { // 10个寄存器
				v = append(v, int64(i))
			}
			point := Point{
				Data:      v,
				DeviceId:  DeviceId(i),
				Timestamp: int64(k),
			}
			points <- &point
		}

	}
	close(points)
	fmt.Println("write ok...")
	time.Sleep(time.Second * 10)
}

func TestRead(t *testing.T) {
	engine := New()
	engine.Init()

	pipeline := engine.OpenIndexPipeline(CompactFiles{
		Key:  "604800000000000_-1_1684168368974",
		Path: "/home/wyatt/code/tmp/data/value/604800000000000/-1/604800000000000_-1_1684168368974",
		Size: 0,
	})
	for _ = range pipeline {
	}
}

func TestExist(t *testing.T) {
	appFS := afero.NewMemMapFs()
	// create test files and directories
	appFS.MkdirAll("src/a", 0755)
	afero.WriteFile(appFS, "src/a/b", []byte("file b"), 0644)
	afero.WriteFile(appFS, "src/c", []byte("file c"), 0644)
	name := "src/c"
	_, err := appFS.Stat(name)
	if os.IsNotExist(err) {
		t.Errorf("file \"%s\" does not exist.\n", name)
	}
}
