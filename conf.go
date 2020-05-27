package main

import (
	"fmt"
)

type Config struct {
	General       GeneralConf       `gcfg:"general"`
	SourceMqtt    SourceMqttConf    `gcfg:"source-mqtt"`
	PipeTopic     PipeTopicConf     `gcfg:"pipe-topic"`
	ClickHouse    ClickHouseConf    `gcfg:"clickhouse"`
	MonitorMqtt   MonitorMqttConf   `gcfg:"monitor-mqtt"`
	MonitorInfo   MonitorInfoConf   `gcfg:"monitor-info"`
	PipeInfo      PipeInfoConf      `gcfg:"pipe-info"`
	ProcessorInfo ProcessorInfoConf `gcfg:"processor-info"`
	AdapterInfo   AdapterInfoConf   `gcfg:"adapter-info"`
	DbStoreInfo   DbStoreInfoConf   `gcfg:"dbstore-info"`
}

type GeneralConf struct {
	Debug         bool
	Sleepinterval uint
}

type SourceMqttConf struct {
	Scheme       string
	Hostname     string
	Port         uint
	Cleansession bool
	Qos          uint8
	Pingtimeout  uint8
	Keepalive    uint16
	Username     string
	Password     string
	Topicroot    string
}

type PipeTopicConf struct {
	Targetname     string
	Topicprefix    string
	Enablegroupnum bool
	Begingroupnum  uint16
	Endgroupnum    uint16
}

type ClickHouseConf struct {
	Scheme   string
	Hostname string
	Port     uint
	Username string
	Password string
	Database string
	Compress bool
	Debug    bool
}

type MonitorMqttConf struct {
	Scheme       string
	Hostname     string
	Port         uint
	Cleansession bool
	Qos          uint8
	Pingtimeout  uint8
	Keepalive    uint16
	Username     string
	Password     string
	Topicroot    string
}

type MonitorInfoConf struct {
	Buffersize      uint
	Publishinterval uint8
}

type PipeInfoConf struct {
	Pipeidmaxlen uint8
	Taskinterval uint8
	Buffersize   uint
}

type ProcessorInfoConf struct {
	Messageidlength uint8
	Buffersize      uint
}
type AdapterInfoConf struct {
	Adapter          string
	Rawtablename     string
	Adaptertablename string
	Jsonsample       string
	Buffersize       uint
}

type DbStoreInfoConf struct {
	Buffersize uint
}

func (cfg *Config) GetConfInfo() string {
	info := fmt.Sprintf("Configuration information ... \n [general] => %+v\n [source-mqtt] => %+v\n [pipe-topic] => %+v\n [clickhouse] => %+v\n [monitor-mqtt] => %+v\n [monitor-info] => %+v\n [pipe-info] => %+v\n [processor-info] => %+v\n [adapter-info] => %+v\n [dbstore-info] => %+v\n ", cfg.General, cfg.SourceMqtt, cfg.PipeTopic, cfg.ClickHouse, cfg.MonitorMqtt, cfg.MonitorInfo, cfg.PipeInfo, cfg.ProcessorInfo, cfg.AdapterInfo, cfg.DbStoreInfo)
	return info
}
