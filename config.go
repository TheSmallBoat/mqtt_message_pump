package main

import (
	"os"
	"runtime"
	"strings"

	log "github.com/Sirupsen/logrus"
	gcfg "gopkg.in/gcfg.v1"
)

type GeneralConf struct {
	Debug         bool
	Sleepinterval uint
}

type Config struct {
	General     GeneralConf
	SourceMqtt  SourceMqttConf  `gcfg:"source-mqtt"`
	PipeTopic   PipeTopicConf   `gcfg:"pipe-topic"`
	ClickHouse  ClickHouseConf  `gcfg:"clickhouse"`
	MonitorMqtt MonitorMqttConf `gcfg:"monitor-mqtt"`
	MonitorInfo MonitorInfoConf `gcfg:"monitor-info"`
	PipeInfo    PipeInfoConf    `gcfg:"pipe-info"`
}

func UserHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

func LoadConf(path string) (*Config, error) {
	home := UserHomeDir()
	path = strings.Replace(path, "~", home, 1)

	var cfg Config
	err := gcfg.ReadFileInto(&cfg, path)
	if err != nil {
		return nil, err
	}

	if cfg.General.Debug {
		log.SetLevel(log.DebugLevel)
		log.Debug("Now enable the debug mode ... ")
	}
	return &cfg, nil
}
