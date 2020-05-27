package main

import (
	"fmt"
	"time"
)

type MonitorInfoConf struct {
	Buffersize      uint
	PublishInterval uint8
}

type ClickHouseBlockRunInfo struct {
	BlockMessageSize uint
	BlockPrepareTime uint32
	BlockCommitTime  uint32
	BlockRunTime     uint32
}

type Monitor struct {
	MqttPublishClient *MqttPubSubClient
	PubInterval       uint8
	DbBlockComSize    uint
	PubFailNum        uint
	PubSucceedNum     uint

	PipeChan chan bool // Pipe channel of the pump.
	PipeNum  uint      // Total number of the pipe channels of the pump.

	SubChan       chan uint // Subscribe message channel of the pump.
	SubMsgFailed  uint
	SubMsgSucceed uint
	SubMsgPerSec  uint
	SubMsgSize    uint

	FwdChan       chan uint // Forward message channel of pumping data to clickhouse.
	FwdMsgFailed  uint
	FwdMsgSucceed uint
	FwdMsgPerSec  uint
	FwdMsgSize    uint

	ChErrChan chan bool // clickhouse channel of the pump. True means failed.
	ChsErrNum uint

	ChRunInfoChan   chan ClickHouseBlockRunInfo
	ChBlkPreTimLast uint32
	ChBlkComTimLast uint32
	ChBlkRunTimLast uint32
	ChBlkPreTimAvg  uint32
	ChBlkComTimAvg  uint32
	ChBlkRunTimAvg  uint32
	ChBlkSucceed    uint32
	ChsMsgPerSec    uint
	ChsMsgSucceed   uint
}

func NewMonitor(cfg *Config) (*Monitor, error) {
	mmc, err := NewMonitorMqttClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("monitor mqtt publish client init err: %s", err)
	}
	return &Monitor{
		MqttPublishClient: mmc,
		PubInterval:       cfg.MonitorInfo.PublishInterval,
		DbBlockComSize:    cfg.ClickHouse.Blocksize,
		PipeChan:          make(chan bool, cfg.MonitorInfo.Buffersize),
		SubChan:           make(chan uint, cfg.MonitorInfo.Buffersize),
		FwdChan:           make(chan uint, cfg.MonitorInfo.Buffersize),
		ChErrChan:         make(chan bool, cfg.MonitorInfo.Buffersize),
		ChRunInfoChan:     make(chan ClickHouseBlockRunInfo, cfg.MonitorInfo.Buffersize),
		ChBlkComTimAvg:    1000, // The variable cannot be zero, due to be divisor.
	}, nil
}

func (mon *Monitor) publishInfo(payload string) {
	topic := mon.MqttPublishClient.Topic
	qos := byte(mon.MqttPublishClient.Qos)
	if token := mon.MqttPublishClient.MqttClient.Publish(topic, qos, false, payload); token.Wait() && token.Error() != nil {
		mon.PubFailNum++
	}
	mon.PubSucceedNum++
}

func (mon *Monitor) Start() {
	defer mon.MqttPublishClient.Disconnect()

	ticker := time.NewTicker(time.Duration(mon.PubInterval) * time.Second)
	begin := time.Now()

	for {
		select {
		case <-ticker.C:
			intv := uint(mon.PubInterval)
			runtime := uint(time.Now().Sub(begin).Seconds())
			avgsmps := (mon.SubMsgSucceed + mon.SubMsgFailed) / runtime
			avgfmps := (mon.FwdMsgSucceed + mon.FwdMsgFailed) / runtime
			avgdmps := mon.ChsMsgSucceed / runtime

			estimateMsgPerDay := 24 * 1200 * (avgsmps + avgfmps + avgdmps)
			estimateMsgSizePerDay := 24 * 1800 * (mon.SubMsgPerSec + mon.FwdMsgPerSec) / intv / 1024 / 1024
			estimateDbBlockCommitPerSec := 1000 / mon.ChBlkComTimAvg
			estimateDbMsgCommitPerSec := mon.DbBlockComSize * uint(estimateDbBlockCommitPerSec)

			// The difference gap between the number of subscribes messages per second and the number of DB commits per second.
			// It's very helpful for watching the performance and finding how many spaces to improve via this metric.
			diffMsgDbSubPerSec := estimateDbMsgCommitPerSec - avgsmps

			infFmt := "{\"RunTime(s)\":%d,\"PipeNum\":%d,\"MonPubSucceed\":%d,\"MonPubFailed\":%d}"
			inf := fmt.Sprintf(infFmt, runtime, mon.PipeNum, mon.PubSucceedNum, mon.PubFailNum)

			subMsgFmt := "{\"SubMsgPerSec\":%d,\"SubMsgSizePerSec\":%d,\"SubMsgSucceed\":%d,\"SubMsgFailed\":%d,\"AvgPeriodSubMsgPerSec\":%d}"
			subMsg := fmt.Sprintf(subMsgFmt, mon.SubMsgPerSec/intv, mon.SubMsgSize/intv, mon.SubMsgSucceed, mon.SubMsgFailed, avgsmps)

			fwdMsgFmt := "{\"FwdMsgPerSec\":%d,\"FwdMsgSizePerSec\":%d,\"FwdMsgSucceed\":%d,\"FwdMsgFailed\":%d,\"AvgPeriodFwdMsgPerSec\":%d}"
			fwdMsg := fmt.Sprintf(fwdMsgFmt, mon.FwdMsgPerSec/intv, mon.FwdMsgSize/intv, mon.FwdMsgSucceed, mon.FwdMsgFailed, avgfmps)

			dbMsgFmt := "{\"DbMsgPerSec\":%d,\"DbMsgSucceed\":%d,\"DbErrorNum\":%d,\"AvgPeriodDbMsgPerSec\":%d}"
			dbMsg := fmt.Sprintf(dbMsgFmt, mon.ChsMsgPerSec/intv, mon.ChsMsgSucceed, mon.ChsErrNum, avgdmps)

			dbBlkFmt := "{\"CommitSize\":%d,\"Succeed\":%d,\"PrepareTimeLast(ms)\":%d,\"CommitTimeLast(ms)\":%d,\"RunTimeLast(ms)\":%d,\"PrepareTimeAvg(ms)\":%d,\"CommitTimeAvg(ms)\":%d,\"RunTimeAvg(ms)\":%d}"
			dbBlk := fmt.Sprintf(dbBlkFmt, mon.DbBlockComSize, mon.ChBlkSucceed, mon.ChBlkPreTimLast, mon.ChBlkComTimLast, mon.ChBlkRunTimLast, mon.ChBlkPreTimAvg, mon.ChBlkComTimAvg, mon.ChBlkRunTimAvg)

			esMPDFmt := "{\"MessageItemsPerDay\":%d,\"MessageSizePerDay(MB)\":%d,\"DbBlockCommitPerSec\":%d,\"DbMessageCommitPerSec\":%d,\"DiffMessageDbSubPerSec\":%d}"
			esMPD := fmt.Sprintf(esMPDFmt, estimateMsgPerDay, estimateMsgSizePerDay, estimateDbBlockCommitPerSec, estimateDbMsgCommitPerSec, diffMsgDbSubPerSec)

			plFmt := "{\"Info\":%s,\"SubscribeMsgInfo\":%s,\"ForwardMsgInfo\":%s,\"DbMsgInfo\":%s,\"DbBlockInfo\":%s,\"EstimateInfo\":%s}"
			payload := fmt.Sprintf(plFmt, inf, subMsg, fwdMsg, dbMsg, dbBlk, esMPD)

			mon.publishInfo(payload)
			mon.SubMsgPerSec = 0
			mon.SubMsgSize = 0
			mon.FwdMsgPerSec = 0
			mon.FwdMsgSize = 0
			mon.ChsMsgPerSec = 0
		case flagForward := <-mon.FwdChan:
			mon.FwdMsgPerSec++
			if flagForward > 0 {
				mon.FwdMsgSucceed++
				mon.FwdMsgSize += flagForward
			} else {
				mon.FwdMsgFailed++
			}
		case flagSub := <-mon.SubChan:
			mon.SubMsgPerSec++
			if flagSub > 0 {
				mon.SubMsgSucceed++
				mon.SubMsgSize += flagSub
			} else {
				mon.SubMsgFailed++
			}
		case flagPipe := <-mon.PipeChan:
			if flagPipe {
				mon.PipeNum++
			} else {
				mon.PipeNum--
			}
		case flagCh := <-mon.ChErrChan:
			if flagCh {
				mon.ChsErrNum++
			}
		case chRunInfo, ok := <-mon.ChRunInfoChan:
			if ok {
				mon.ChBlkPreTimLast = chRunInfo.BlockPrepareTime
				mon.ChBlkComTimLast = chRunInfo.BlockCommitTime
				mon.ChBlkRunTimLast = chRunInfo.BlockRunTime
				mon.ChBlkPreTimAvg = (mon.ChBlkPreTimLast + mon.ChBlkPreTimAvg*mon.ChBlkSucceed) / (mon.ChBlkSucceed + 1)
				mon.ChBlkComTimAvg = (mon.ChBlkComTimLast + mon.ChBlkComTimAvg*mon.ChBlkSucceed) / (mon.ChBlkSucceed + 1)
				mon.ChBlkRunTimAvg = (mon.ChBlkRunTimLast + mon.ChBlkRunTimAvg*mon.ChBlkSucceed) / (mon.ChBlkSucceed + 1)
				mon.ChBlkSucceed++
				mon.ChsMsgPerSec += chRunInfo.BlockMessageSize
				mon.ChsMsgSucceed += chRunInfo.BlockMessageSize
			}
		}
	}
}
