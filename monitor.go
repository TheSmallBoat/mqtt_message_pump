package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

const MinMonitorChannelBufferSize = uint(16)
const MaxMonitorChannelBufferSize = uint(256)

type Monitor struct {
	MqttPublishClient *MqttPubSubClient
	PubInterval       uint8
	PubFailNum        uint32
	PubSucceedNum     uint32

	PipeChan chan bool // Pipe channel of the pump.
	PipeNum  int32     // Total number of the pipe channels of the pump.

	SubChan       chan uint32 // Subscribe message channel of the pump.
	SubMsgPerSec  uint32
	SubMsgFailed  uint64
	SubMsgSucceed uint64
	SubMsgSize    uint64

	DataProcessChan chan uint32 // The data process channel
	DpMsgPerSec     uint32
	DpMsgFailed     uint64
	DpMsgSucceed    uint64
	DpMsgSize       uint64

	DataAdapterChan chan uint32 // The data adapter channel
	DaMsgPerSec     uint32
	DaMsgFailed     uint64
	DaMsgSucceed    uint64
	DaMsgSize       uint64

	DataAdapterPDOChan chan uint32 // The data process object channel of the data adapter
	DaPdoMsgPerSec     uint32
	DaPdoMsgSucceed    uint64
	DaPdoMsgFailed     uint64

	DBStoreRawDataObjectReceiveChan chan bool // The data db store receive channel of the raw data object
	DbsRDOReceivePerSec             uint32
	DbsRDOReceiveSucceed            uint64
	DbsRDOReceiveFailed             uint64

	DBStoreAdapterDataObjectReceiveChan chan bool // The data db store receive channel of the adapter data object
	DbsADOReceivePerSec                 uint32
	DbsADOReceiveSucceed                uint64
	DbsADOReceiveFailed                 uint64

	DBStoreRawDataObjectCommitChan chan uint32 // The data db store commit channel of the raw data object
	DbsRDOCommitPerSec             uint32
	DbsRDOCommitSucceed            uint64
	DbsRDOCommitFailed             uint64

	DBStoreAdapterDataObjectCommitChan chan uint32 // The data db store commit channel of the adapter data object
	DbsADOCommitPerSec                 uint32
	DbsADOCommitSucceed                uint64
	DbsADOCommitFailed                 uint64

	DBStoreErrChan chan bool // The error channel for data base store
	DBStoreErrNum  uint32
}

func NewMonitor(cfg *Config, m *Metrics) (*Monitor, error) {
	mmc, err := NewMonitorMqttClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("monitor mqtt publish client init err: %s", err)
	}

	bufferSize := MinMonitorChannelBufferSize
	if cfg.MonitorInfo.Buffersize > bufferSize {
		bufferSize = cfg.MonitorInfo.Buffersize
		if bufferSize > MaxMonitorChannelBufferSize {
			bufferSize = MaxMonitorChannelBufferSize
		}
	}
	m.ChannelBufferSize.Monitor = bufferSize

	return &Monitor{
		MqttPublishClient:                   mmc,
		PubInterval:                         cfg.MonitorInfo.Publishinterval,
		PipeChan:                            make(chan bool, bufferSize),
		SubChan:                             make(chan uint32, bufferSize),
		DataProcessChan:                     make(chan uint32, bufferSize),
		DataAdapterChan:                     make(chan uint32, bufferSize),
		DataAdapterPDOChan:                  make(chan uint32, bufferSize),
		DBStoreRawDataObjectReceiveChan:     make(chan bool, bufferSize),
		DBStoreRawDataObjectCommitChan:      make(chan uint32, bufferSize),
		DBStoreAdapterDataObjectReceiveChan: make(chan bool, bufferSize),
		DBStoreAdapterDataObjectCommitChan:  make(chan uint32, bufferSize),
		DBStoreErrChan:                      make(chan bool, bufferSize),
	}, nil
}

func (mon *Monitor) publishInfo(payload string) {
	topic := mon.MqttPublishClient.Topic
	qos := mon.MqttPublishClient.Qos
	if token := mon.MqttPublishClient.MqttClient.Publish(topic, qos, false, payload); token.Wait() && token.Error() != nil {
		atomic.AddUint32(&mon.PubFailNum, 1)
	}
	atomic.AddUint32(&mon.PubSucceedNum, 1)
}

func (mon *Monitor) Start() {
	defer mon.MqttPublishClient.Disconnect()

	ticker := time.NewTicker(time.Duration(mon.PubInterval) * time.Second)
	begin := time.Now()

	for {
		select {
		case <-ticker.C:
			intv := mon.PubInterval
			runtime := time.Now().Sub(begin).Seconds()

			waitNumForDataProcess := mon.SubMsgSucceed - mon.DpMsgSucceed

			waitNumForRawDataObjectToDB := mon.DpMsgSucceed - mon.DbsRDOReceiveSucceed
			waitNumForRawDataObjectToDBCommit := mon.DbsRDOReceiveSucceed - mon.DbsRDOCommitSucceed

			waitNumForAdapter := mon.DpMsgSucceed - mon.DaMsgSucceed
			waitNumForProcessDataObject := mon.DaMsgSucceed - mon.DaPdoMsgSucceed
			waitNumForAdapterDataObjectToDB := mon.DaPdoMsgSucceed - mon.DbsADOReceiveSucceed
			waitNumForAdapterDataObjectToDBCommit := mon.DbsADOReceiveSucceed - mon.DbsADOCommitSucceed

			waitFmt := "{\"WaitForProcess\":%d,\"WaitForRawDataObjectToDB\":%d,\"WaitForRawDataObjectToDBCommit\":%d,\"WaitForAdapter\":%d,\"WaitForProcessDataObject\":%d,\"WaitForAdapterDataObjectToDB\":%d,\"WaitForAdapterDataObjectToDBCommit\":%d}"
			waitInf := fmt.Sprintf(waitFmt, waitNumForDataProcess, waitNumForRawDataObjectToDB, waitNumForRawDataObjectToDBCommit, waitNumForAdapter, waitNumForProcessDataObject, waitNumForAdapterDataObjectToDB, waitNumForAdapterDataObjectToDBCommit)

			gapToDB := int(mon.DbsRDOReceiveSucceed) - int(mon.DbsADOReceiveSucceed)
			gapToDBCommit := int(mon.DbsRDOCommitSucceed) - int(mon.DbsADOCommitSucceed)
			gapFmt := "{\"GapToDB\":%d,\"GapToDBCommit\":%d}"
			gapInf := fmt.Sprintf(gapFmt, gapToDB, gapToDBCommit)

			avgSubMps := float64(mon.SubMsgSucceed+mon.SubMsgFailed) / runtime
			avgDpMps := float64(mon.DpMsgSucceed+mon.DpMsgFailed) / runtime
			avgDaMps := float64(mon.DaMsgSucceed+mon.DaMsgFailed) / runtime
			avgDaPdoMps := float64(mon.DaPdoMsgSucceed+mon.DaPdoMsgFailed) / runtime

			avgDbsRdoRevMps := float64(mon.DbsRDOReceiveSucceed+mon.DbsRDOReceiveFailed) / runtime
			avgDbsRdoComMps := float64(mon.DbsRDOCommitSucceed+mon.DbsRDOCommitFailed) / runtime
			avgDbsAdoRevMps := float64(mon.DbsADOReceiveSucceed+mon.DbsADOReceiveFailed) / runtime
			avgDbsAdoComMps := float64(mon.DbsADOCommitSucceed+mon.DbsADOCommitFailed) / runtime

			infFmt := "{\"RunTime(s)\":%.1f,\"PipeNum\":%d,\"MonPubSucceed\":%d,\"MonPubFailed\":%d}"
			inf := fmt.Sprintf(infFmt, runtime, mon.PipeNum, mon.PubSucceedNum, mon.PubFailNum)

			subMsgFmt := "{\"SubMsgPerSec\":%.1f,\"SubMsgSizePerSec\":%.1f,\"SubMsgSucceed\":%d,\"SubMsgFailed\":%d,\"AvgPeriodSubMsgPerSec\":%.1f}"
			subMsg := fmt.Sprintf(subMsgFmt, float32(mon.SubMsgPerSec)/float32(intv), float32(mon.SubMsgSize)/float32(intv), mon.SubMsgSucceed, mon.SubMsgFailed, avgSubMps)

			dpMsgFmt := "{\"DpMsgPerSec\":%.1f,\"DpMsgSizePerSec\":%.1f,\"DpMsgSucceed\":%d,\"DpMsgFailed\":%d,\"AvgPeriodDpMsgPerSec\":%.1f}"
			dpMsg := fmt.Sprintf(dpMsgFmt, float32(mon.DpMsgPerSec)/float32(intv), float32(mon.DpMsgSize)/float32(intv), mon.DpMsgSucceed, mon.DpMsgFailed, avgDpMps)

			daMsgFmt := "{\"DaMsgPerSec\":%.1f,\"DaMsgSizePerSec\":%.1f,\"DaMsgSucceed\":%d,\"DaMsgFailed\":%d,\"AvgPeriodDaMsgPerSec\":%.1f}"
			daMsg := fmt.Sprintf(daMsgFmt, float32(mon.DaMsgPerSec)/float32(intv), float32(mon.DaMsgSize)/float32(intv), mon.DaMsgSucceed, mon.DaMsgFailed, avgDaMps)

			daPdoMsgFmt := "{\"DaPdoMsgPerSec\":%.1f,\"DaPdoMsgSucceed\":%d,\"DaPdoMsgFailed\":%d,\"AvgPeriodDaPdoMsgPerSec\":%.1f}"
			daPdoMsg := fmt.Sprintf(daPdoMsgFmt, float32(mon.DaPdoMsgPerSec)/float32(intv), mon.DaPdoMsgSucceed, mon.DaPdoMsgFailed, avgDaPdoMps)

			dbsRdoRevFmt := "{\"DbsRDOReceivePerSec\":%.1f,\"DbsRDOReceiveSucceed\":%d,\"DbsRDOReceiveFailed\":%d,\"AvgPeriodDbsRdoRevPerSec\":%.1f}"
			daRdoRev := fmt.Sprintf(dbsRdoRevFmt, float32(mon.DbsRDOReceivePerSec)/float32(intv), mon.DbsRDOReceiveSucceed, mon.DbsRDOReceiveFailed, avgDbsRdoRevMps)

			dbsRdoComFmt := "{\"DbsRDOCommitPerSec\":%.1f,\"DbsRDOCommitSucceed\":%d,\"DbsRDOCommitFailed\":%d,\"AvgPeriodDbsRdoComPerSec\":%.1f}"
			daRdoCom := fmt.Sprintf(dbsRdoComFmt, float32(mon.DbsRDOCommitPerSec)/float32(intv), mon.DbsRDOCommitSucceed, mon.DbsRDOCommitFailed, avgDbsRdoComMps)

			dbsAdoRevFmt := "{\"DbsADOReceivePerSec\":%.1f,\"DbsADOReceiveSucceed\":%d,\"DbsADOReceiveFailed\":%d,\"AvgPeriodDbsAdoRevPerSec\":%.1f}"
			daAdoRev := fmt.Sprintf(dbsAdoRevFmt, float32(mon.DbsADOReceivePerSec)/float32(intv), mon.DbsADOReceiveSucceed, mon.DbsADOReceiveFailed, avgDbsAdoRevMps)

			dbsAdoComFmt := "{\"DbsADOCommitPerSec\":%.1f,\"DbsADOCommitSucceed\":%d,\"DbsADOCommitFailed\":%d,\"AvgPeriodDbsAdoComPerSec\":%.1f}"
			daAdoCom := fmt.Sprintf(dbsAdoComFmt, float32(mon.DbsADOCommitPerSec)/float32(intv), mon.DbsADOCommitSucceed, mon.DbsADOCommitFailed, avgDbsAdoComMps)

			plFmt := "{\"Info\":%s,\"PerformanceMetrics\":{\"QueueWaitInfo\":%s,\"GapBetweenRawAndAdapterInfo\":%s},\"DataInfo\":{\"SubscribeMsgInfo\":%s,\"DataProcessMsgInfo\":%s,\"DataAdapterInfo\":%s,\"DataProcessDataObjectInfo\":%s},\"DataDBStoreInfo\":{\"CheckErrors\":%d,\"RawDataObjectReceiveInfo\":%s,\"RawDataObjectCommitInfo\":%s,\"AdapterDataObjectReceiveInfo\":%s,\"AdapterDataObjectCommitInfo\":%s}}"
			payload := fmt.Sprintf(plFmt, inf, waitInf, gapInf, subMsg, dpMsg, daMsg, daPdoMsg, mon.DBStoreErrNum, daRdoRev, daRdoCom, daAdoRev, daAdoCom)

			mon.publishInfo(payload)
			mon.SubMsgPerSec = 0
			mon.SubMsgSize = 0
			mon.DpMsgPerSec = 0
			mon.DpMsgSize = 0
			mon.DaMsgPerSec = 0
			mon.DaMsgSize = 0
			mon.DaPdoMsgPerSec = 0
			mon.DbsRDOReceivePerSec = 0
			mon.DbsADOReceivePerSec = 0
			mon.DbsRDOCommitPerSec = 0
			mon.DbsADOCommitPerSec = 0

		case flagSub := <-mon.SubChan:
			atomic.AddUint32(&mon.SubMsgPerSec, 1)
			if flagSub > 0 {
				atomic.AddUint64(&mon.SubMsgSucceed, 1)
				atomic.AddUint64(&mon.SubMsgSize, uint64(flagSub))
			} else {
				atomic.AddUint64(&mon.SubMsgFailed, 1)
			}
		case flagDataProcess := <-mon.DataProcessChan:
			atomic.AddUint32(&mon.DpMsgPerSec, 1)
			if flagDataProcess > 0 {
				atomic.AddUint64(&mon.DpMsgSucceed, 1)
				atomic.AddUint64(&mon.DpMsgSize, uint64(flagDataProcess))
			} else {
				atomic.AddUint64(&mon.DpMsgFailed, 1)
			}
		case flagDataAdapter := <-mon.DataAdapterChan:
			atomic.AddUint32(&mon.DaMsgPerSec, 1)
			if flagDataAdapter > 0 {
				atomic.AddUint64(&mon.DaMsgSucceed, 1)
				atomic.AddUint64(&mon.DaMsgSize, uint64(flagDataAdapter))
			} else {
				atomic.AddUint64(&mon.DaMsgFailed, 1)
			}
		case flagDataAdapterPDO := <-mon.DataAdapterPDOChan:
			atomic.AddUint32(&mon.DaPdoMsgPerSec, 1)
			if flagDataAdapterPDO > 0 {
				atomic.AddUint64(&mon.DaPdoMsgSucceed, 1)
			} else {
				atomic.AddUint64(&mon.DaPdoMsgFailed, 1)
			}
		case flagDbStoreRawDOR := <-mon.DBStoreRawDataObjectReceiveChan:
			atomic.AddUint32(&mon.DbsRDOReceivePerSec, 1)
			if flagDbStoreRawDOR {
				atomic.AddUint64(&mon.DbsRDOReceiveSucceed, 1)
			} else {
				atomic.AddUint64(&mon.DbsRDOReceiveFailed, 1)
			}
		case flagDbStoreRawDOC := <-mon.DBStoreRawDataObjectCommitChan:
			atomic.AddUint32(&mon.DbsRDOCommitPerSec, 1)
			if flagDbStoreRawDOC > 0 {
				atomic.AddUint64(&mon.DbsRDOCommitSucceed, 1)
			} else {
				atomic.AddUint64(&mon.DbsRDOCommitFailed, 1)
			}
		case flagDbStoreAdapterDOR := <-mon.DBStoreAdapterDataObjectReceiveChan:
			atomic.AddUint32(&mon.DbsADOReceivePerSec, 1)
			if flagDbStoreAdapterDOR {
				atomic.AddUint64(&mon.DbsADOReceiveSucceed, 1)
			} else {
				atomic.AddUint64(&mon.DbsADOReceiveFailed, 1)
			}
		case flagDbStoreAdapterDOC := <-mon.DBStoreAdapterDataObjectCommitChan:
			atomic.AddUint32(&mon.DbsADOCommitPerSec, 1)
			if flagDbStoreAdapterDOC > 0 {
				atomic.AddUint64(&mon.DbsADOCommitSucceed, 1)
			} else {
				atomic.AddUint64(&mon.DbsADOCommitFailed, 1)
			}
		case flagPipe := <-mon.PipeChan:
			if flagPipe {
				atomic.AddInt32(&mon.PipeNum, 1)
			} else {
				atomic.AddInt32(&mon.PipeNum, -1)
			}
		case flagErr := <-mon.DBStoreErrChan:
			if flagErr {
				atomic.AddUint32(&mon.DBStoreErrNum, 1)
			}
		}
	}
}
