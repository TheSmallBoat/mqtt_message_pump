package main

import (
	"crypto/rand"
	"fmt"
)

const MinMessageIdLength = uint8(8)
const MinDataProcessorChannelBufferSize = uint(8)
const MaxDataProcessorChannelBufferSize = uint(128)

type DataProcessor struct {
	Monitor           *Monitor
	DataAdapter       *DataAdapter
	ClickHouseDBStore *ClickHouseDBStore

	DataChan                  chan Message
	DataRawObjectDBChan       *chan DataRawDBObject
	DataNeedAdapterObjectChan *chan DataNeedAdapterObject

	MessageIdLength uint8
}

func getRandomMessageId(maxMessageIdLen uint8) string {
	const alphaNum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var bytes = make([]byte, maxMessageIdLen)
	_, _ = rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphaNum[b%byte(len(alphaNum))]
	}
	return string(bytes)
}

func NewDataProcessor(cfg *Config, mon *Monitor, m *Metrics, da *DataAdapter, chDBS *ClickHouseDBStore) (*DataProcessor, error) {
	idLength := MinMessageIdLength
	if cfg.ProcessorInfo.Messageidlength > MinMessageIdLength {
		idLength = cfg.ProcessorInfo.Messageidlength
	}
	bufferSize := MinDataProcessorChannelBufferSize
	if cfg.ProcessorInfo.Buffersize > bufferSize {
		bufferSize = cfg.ProcessorInfo.Buffersize
		if bufferSize > MaxDataProcessorChannelBufferSize {
			bufferSize = MaxDataProcessorChannelBufferSize
		}
	}
	m.ChannelBufferSize.DataProcessor = bufferSize

	da.DataAdapterObjectDBChan = &chDBS.DataAdapterObjectDBChan
	if da.DataAdapterObjectDBChan == nil {
		return nil, fmt.Errorf("new data processor error : DataAdapterObjectDBChan is nil")
	}

	dp := &DataProcessor{
		Monitor:                   mon,
		DataAdapter:               da,
		ClickHouseDBStore:         chDBS,
		DataChan:                  make(chan Message, bufferSize),
		DataRawObjectDBChan:       &chDBS.DataRawObjectDBChan,
		DataNeedAdapterObjectChan: &da.DataNeedAdapterObject,
		MessageIdLength:           idLength,
	}

	if dp.DataRawObjectDBChan != nil && dp.DataNeedAdapterObjectChan != nil {
		return dp, nil
	} else {
		return dp, fmt.Errorf("new data processor error : DataRawObjectDBChan or DataNeedAdapterObjectChan is nil")
	}
}

func (dp *DataProcessor) StartDataProcessTaskWithAdapter() {
	for {
		select {
		case msg, ok := <-dp.DataChan:
			if !ok {
				dp.Monitor.DataProcessChan <- 0
			} else {
				mid := getRandomMessageId(dp.MessageIdLength)
				*dp.DataNeedAdapterObjectChan <- DataNeedAdapterObject{mid, msg.Payload, msg.ReceivedTime}
				*dp.DataRawObjectDBChan <- DataRawDBObject{mid, msg}
				dp.Monitor.DataProcessChan <- uint32(len(msg.Topic) + len(msg.Payload) + 4)
			}
		}
	}
}

func (dp *DataProcessor) StartDataProcessTaskWithNoAdapter() {
	for {
		select {
		case msg, ok := <-dp.DataChan:
			if !ok {
				dp.Monitor.DataProcessChan <- 0
			} else {
				mid := getRandomMessageId(dp.MessageIdLength)
				*dp.DataRawObjectDBChan <- DataRawDBObject{mid, msg}
				dp.Monitor.DataProcessChan <- uint32(len(msg.Topic) + len(msg.Payload) + 4)
			}
		}
	}
}
