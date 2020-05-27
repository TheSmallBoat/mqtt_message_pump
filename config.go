package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/gcfg.v1"
)

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
	log.Info(fmt.Sprintf("Loading configuration information from '%s' ", path))

	var cfg Config
	err := gcfg.ReadFileInto(&cfg, path)
	if err != nil {
		return nil, err
	}
	log.Info(cfg.GetConfInfo())

	if cfg.General.Debug {
		log.SetLevel(log.DebugLevel)
		log.Debug("The debug mode ... [ENABLE]")
	} else {
		log.SetLevel(log.InfoLevel)
	}
	return &cfg, nil
}
