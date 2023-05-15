package analyzer

import (
	"encoding/json"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"time"
)

type (
	SolarAnalyzer struct{}
)

type SolarMessage struct {
	MsgType   int   `json:"msg_type"`
	Status    int   `json:"status"`
	MsgID     int   `json:"msg_id"`
	Timestamp int64 `json:"timestamp"`
	Data      any   `json:"data"`
}

type SolarData struct {
	Timestamp  int64           `mapstructure:"timestamp" json:"timestamp"`
	Regs       Regs            `mapstructure:"regs" json:"regs"`
	SlaveStat  int             `mapstructure:"slave_stat" json:"slave_stat"`
	DevStat    int             `mapstructure:"dev_stat" json:"dev_stat"`
	SlaveInfo  [][]interface{} `mapstructure:"slave_info" json:"slave_info"`
	Sn         string          `mapstructure:"sn" json:"sn"`
	Version    string          `mapstructure:"version" json:"version"`
	SubVersion int             `mapstructure:"sub_version" json:"sub_version"`
	RegCfg     string          `mapstructure:"reg_cfg" json:"reg_cfg"`
	Preferred  string          `mapstructure:"preferred" json:"preferred"`
	Type       string          `mapstructure:"type" json:"type"`
	IP         string          `mapstructure:"ip" json:"ip"`
	Gw         string          `mapstructure:"gw" json:"gw"`
	Mask       string          `mapstructure:"mask" json:"mask"`
	DNS1       string          `mapstructure:"dns1" json:"dns1"`
	DNS2       string          `mapstructure:"dns2" json:"dns2"`
	ICID       string          `mapstructure:"ICID" json:"ICID"`
	IMEI       string          `mapstructure:"IMEI" json:"IMEI"`
	Oper       string          `mapstructure:"oper" json:"oper"`
	Lac        string          `mapstructure:"lac" json:"lac"`
	Ci         string          `mapstructure:"ci" json:"ci"`
	ReadyTick  int             `mapstructure:"ready_tick" json:"ready_tick"`
	VerifyTick int             `mapstructure:"verify_tick" json:"verify_tick"`
	SysTick    int             `mapstructure:"sys_tick" json:"sys_tick"`
}

type Regs [][]int

func (s SolarMessage) GetSolarData() *SolarData {
	if s.Data == nil {
		return nil
	}
	v := &SolarData{}
	mapstructure.Decode(s.Data.(map[string]any), v)
	return v
}

func (s SolarMessage) GetRegs() Regs {
	return s.Data.(Regs)
}

func (s *SolarAnalyzer) Analyze(args Message) ([]Record, error) {
	var solarMessage SolarMessage

	err := json.Unmarshal(args.Bytes, &solarMessage)
	if err != nil {
		return nil, errors.Annotatef(err, "%v %v", string(args.Bytes), args)
	}

	updatedAt := args.CreatedAt
	if solarMessage.Timestamp != 0 {
		updatedAt = time.UnixMilli(solarMessage.Timestamp)
	} else if solarMessage.MsgType == 80 {
		data := solarMessage.GetSolarData()
		if data != nil {
			updatedAt = time.UnixMilli(data.Timestamp)
		}
	}

	if updatedAt.Unix() < 0 || updatedAt.Unix() > time.Now().Add(Timeout).Unix() {
		return nil, errors.Annotatef(errors.Timeout, "%v %v", string(args.Bytes), args)
	}

	item := Record{
		DeviceId:  args.DeviceId,
		Data:      solarMessage,
		CreatedAt: args.CreatedAt,
		UpdatedAt: updatedAt,
		MsgType:   solarMessage.MsgType,
	}

	records := []Record{item}

	return records, nil
}

type Message struct {
	Offset    int
	CreatedAt time.Time
	DeviceId  int
	Bytes     []byte
	Length    int
}

type Analyzer interface {
	Analyze(args Message) ([]Record, error)
}

type (
	Record struct {
		DeviceId  int
		Data      any
		CreatedAt time.Time
		UpdatedAt time.Time
		MsgType   int
	}
	MsgType int
)

var Timeout = time.Hour
