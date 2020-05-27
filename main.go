package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/mattn/go-colorable"
	"github.com/urfave/cli"
)

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.TextFormatter{ForceColors: true, FullTimestamp: true})
	log.SetOutput(colorable.NewColorableStdout())
}

func processConfiguration(c *cli.Context) (*Config, error) {
	path := c.String("c")
	cfg, err := LoadConf(path)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	log.Info(fmt.Sprintf("General Configuration: %+v", cfg.General))
	log.Info(fmt.Sprintf("SourceMqtt Configuration: %+v", cfg.SourceMqtt))
	log.Info(fmt.Sprintf("PipeTopic Configuration: %+v", cfg.PipeTopic))
	log.Info(fmt.Sprintf("ClickHouse Configuration: %+v", cfg.ClickHouse))
	log.Info(fmt.Sprintf("MonitorMqtt Configuration: %+v", cfg.MonitorMqtt))
	log.Info(fmt.Sprintf("MonitorInfo Configuration: %+v", cfg.MonitorInfo))
	log.Info(fmt.Sprintf("PipeInfo Configuration: %+v", cfg.PipeInfo))
	return cfg, nil
}

func showConfiguration(c *cli.Context) {
	_, _ = processConfiguration(c)
}

func runPump(cont *cli.Context) {
	cfg, _ := processConfiguration(cont)
	mon, err := NewMonitor(cfg)
	if err != nil {
		log.Fatal(err)
	}

	go mon.Start()
	time.Sleep(time.Duration(cfg.General.Sleepinterval) * time.Millisecond)

	chdb, err := NewClickHouseDBClient(cfg, mon)
	if err != nil {
		log.Fatal(err)
	}
	go chdb.start()
	time.Sleep(time.Duration(cfg.General.Sleepinterval) * time.Millisecond)

	wg := sync.WaitGroup{}
	if cfg.PipeTopic.Enablegroupnum {
		for idx := cfg.PipeTopic.Begingroupnum; idx <= cfg.PipeTopic.Endgroupnum; idx++ {
			wg.Add(1)
			time.Sleep(time.Duration(cfg.PipeInfo.Taskinterval) * time.Millisecond)
			pp := NewPipe(cfg, idx, mon, &chdb.ChMsgChan)
			go pp.start()
		}
	} else {
		wg.Add(1)
		pp := NewPipe(cfg, 0, mon, &chdb.ChMsgChan)
		go pp.start()
	}
	wg.Wait()
}

func main() {
	app := cli.NewApp()
	app.Name = "message pump"
	app.Version = "19.06.25"
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{
			Name: "Abe Cai",
		},
	}
	app.Usage = "A command-line simulator for generating messages published to the MQTT broker."

	app.Commands = []cli.Command{
		{
			Name:   "run",
			Usage:  "pump the messages.",
			Action: runPump,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config, c",
					Usage: "config file path",
					Value: "~/.message-pump.ini",
				},
			},
		},
		{
			Name:   "show",
			Usage:  "print the configuration information.",
			Action: showConfiguration,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config, c",
					Usage: "config file path",
					Value: "~/.message-pump.ini",
				},
			},
		},
	}

	_ = app.Run(os.Args)
}
