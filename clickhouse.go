package main

import (
	"database/sql/driver"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/kshvakov/clickhouse"
	"github.com/kshvakov/clickhouse/lib/data"
	"time"
)

type ClickHouseConf struct {
	Scheme     string
	Hostname   string
	Port       uint
	Username   string
	Password   string
	Database   string
	Compress   uint8
	Blocksize  uint
	Buffersize uint
}

type ClickHouseDBClient struct {
	ChClient clickhouse.Clickhouse
	Monitor  *Monitor

	ChMsgChan chan Message
	DataBlock *data.Block

	Blocksize uint
	MsgIndex  uint
	TableName string

	FlagUxTime uint64
}

func NewClickHouseDBClient(cfg *Config, mon *Monitor) (*ClickHouseDBClient, error) {
	tablename := fmt.Sprintf("%s.raw_message", cfg.ClickHouse.Database)

	dsn := fmt.Sprintf("%s://%s:%d?username=%s&debug=%t&compress=%d", cfg.ClickHouse.Scheme, cfg.ClickHouse.Hostname, cfg.ClickHouse.Port, cfg.ClickHouse.Username, cfg.General.Debug, cfg.ClickHouse.Compress)
	log.Infof("DSN: %s", dsn)

	connect, err := clickhouse.OpenDirect(dsn)
	if err != nil {
		mon.ChErrChan <- true
		return nil, fmt.Errorf("clickhouse db client open direct error: %s", err)
	}
	{
		_, err = connect.Begin()
		if err != nil {
			return nil, fmt.Errorf("clickhouse db client begin error: %s", err)
		}

		query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS  %s`, cfg.ClickHouse.Database)
		log.Info(query)

		stmt, err := connect.Prepare(query)
		if err != nil {
			return nil, fmt.Errorf("clickhouse db client prepare error: %s", err)
		}

		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			mon.ChErrChan <- true
			return nil, fmt.Errorf("clickhouse db client stmt exec error: %s", err)
		}

		if err := connect.Commit(); err != nil {
			mon.ChErrChan <- true
			return nil, fmt.Errorf("clickhouse db client commit error: %s", err)
		}
	}
	{
		_, err = connect.Begin()
		if err != nil {
			return nil, fmt.Errorf("clickhouse db client begin error: %s", err)
		}
		query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
				topic String COMMENT 'message topic',
				payload String COMMENT 'message payloads',
  				collect_datetime DateTime COMMENT 'Date and time of collecting data from the message hub',
  				collect_date Date MATERIALIZED toDate(collect_datetime) COMMENT 'date of collecting data from the message hub'
			) ENGINE = MergeTree(collect_date, (topic, collect_date), 8192)`, tablename)

		log.Infof("CREATE TABLE IF NOT EXISTS %s", tablename)
		stmt, err := connect.Prepare(query)
		if err != nil {
			return nil, fmt.Errorf("clickhouse db client prepare error: %s", err)
		}

		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			mon.ChErrChan <- true
			return nil, fmt.Errorf("clickhouse db client stmt exec error: %s", err)
		}

		if err := connect.Commit(); err != nil {
			mon.ChErrChan <- true
			return nil, fmt.Errorf("clickhouse db client commit error: %s", err)
		}
	}

	chc := &ClickHouseDBClient{
		ChClient:   connect,
		Monitor:    mon,
		ChMsgChan:  make(chan Message, cfg.ClickHouse.Buffersize),
		DataBlock:  nil,
		Blocksize:  cfg.ClickHouse.Blocksize,
		MsgIndex:   0,
		TableName:  tablename,
		FlagUxTime: 0,
	}

	chc.resetBlock()
	return chc, nil
}

func (chc *ClickHouseDBClient) resetBlock() {
	chc.FlagUxTime = uint64(time.Now().UnixNano())
	chc.MsgIndex = 0

	_, err := chc.ChClient.Begin()
	chc.checkError(err)

	query := fmt.Sprintf("INSERT INTO  %s (topic, payload, collect_datetime) VALUES (?, ?, ?)", chc.TableName)
	_, err = chc.ChClient.Prepare(query)
	chc.checkError(err)

	block, err := chc.ChClient.Block()
	chc.checkError(err)

	block.Reserve()
	block.NumRows = uint64(chc.Blocksize)
	chc.DataBlock = block
}

func (chc *ClickHouseDBClient) checkError(err error) {
	if err != nil {
		chc.Monitor.ChErrChan <- true
		log.Errorf("clickhouse db client error: %s", err)
	}
}

func (chc *ClickHouseDBClient) flushBlockData(msg Message) {
	{
		chc.checkError(chc.DataBlock.WriteString(0, msg.Topic))
		chc.checkError(chc.DataBlock.WriteString(1, string(msg.Payload)))
		chc.checkError(chc.DataBlock.WriteUInt32(2, msg.ReceivedTime))
	}
	chc.MsgIndex++
	if chc.MsgIndex >= chc.Blocksize {
		blockPrepareTime := uint64(time.Now().UnixNano()) - chc.FlagUxTime
		if err := chc.ChClient.WriteBlock(chc.DataBlock); err != nil {
			chc.Monitor.ChErrChan <- true
			log.Errorf("clickhouse db client write block error: %s", err)
		}

		if err := chc.ChClient.Commit(); err != nil {
			chc.Monitor.ChErrChan <- true
			log.Errorf("clickhouse db client commit error: %s", err)
		}
		blockRunTime := uint64(time.Now().UnixNano()) - chc.FlagUxTime
		blockCommitTime := blockRunTime - blockPrepareTime

		chc.Monitor.ChRunInfoChan <- ClickHouseBlockRunInfo{
			BlockMessageSize: chc.Blocksize,
			BlockPrepareTime: uint32(blockPrepareTime / 1e6),
			BlockCommitTime:  uint32(blockCommitTime / 1e6),
			BlockRunTime:     uint32(blockRunTime / 1e6),
		}
		chc.resetBlock()
	}
}

func (chc *ClickHouseDBClient) start() {
	for {
		select {
		case msg, ok := <-chc.ChMsgChan:
			if !ok {
				chc.Monitor.FwdChan <- 0
			} else {
				chc.Monitor.FwdChan <- uint(len(msg.Payload) + len(msg.Topic))
				chc.flushBlockData(msg)
			}
		}
	}
}
