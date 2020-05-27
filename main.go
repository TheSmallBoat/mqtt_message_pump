package main

import (
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

func processConfiguration(cont *cli.Context) (*Config, error) {
	path := cont.String("config")
	cfg, err := LoadConf(path)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return cfg, nil
}

func showConfiguration(cont *cli.Context) {
	_, _ = processConfiguration(cont)
}

func NewDataAdapterAndClickHouseDBStoreAndDoCheck(cfg *Config, mon *Monitor, m *Metrics) (*DataAdapter, *ClickHouseDBStore, error) {
	da, err := NewDataAdapter(cfg, mon, m)
	if err != nil {
		return da, nil, err
	}
	item := da.CheckJsonSample(cfg.AdapterInfo.Jsonsample)
	time.Sleep(time.Duration(cfg.General.Sleepinterval) * time.Millisecond)

	chDBS, err := NewClickHouseDBStore(cfg, mon, m, da.MessageTableDDL, &da.MessageTableKeys)
	if err != nil {
		return da, chDBS, err
	} else {
		err = chDBS.DoInsertRawMessageTableForCheck(cfg.AdapterInfo.Jsonsample)
		if err != nil {
			return da, chDBS, err
		}
		err = chDBS.DoQueryRawMessageTableForCheck()
		if err != nil {
			return da, chDBS, err
		}
		err = chDBS.DoInsertAdapterMessageTableForCheck(*item)
		if err != nil {
			return da, chDBS, err
		}
		err = chDBS.DoQueryAdapterMessageTableForCheck()
		if err != nil {
			return da, chDBS, err
		}
	}
	return da, chDBS, nil
}

func checkDataAdapterConfiguration(cont *cli.Context) {
	cfg, _ := processConfiguration(cont)
	m := NewMetrics()
	mon, _ := NewMonitor(cfg, m)
	_, _, _ = NewDataAdapterAndClickHouseDBStoreAndDoCheck(cfg, mon, m)
}

func runPump(cont *cli.Context) {
	cfg, _ := processConfiguration(cont)
	m := NewMetrics()
	mon, err1 := NewMonitor(cfg, m)
	if err1 != nil {
		log.Fatal(err1)
		return
	}

	go mon.Start()
	time.Sleep(time.Duration(cfg.General.Sleepinterval) * time.Millisecond)

	da, chDBS, err2 := NewDataAdapterAndClickHouseDBStoreAndDoCheck(cfg, mon, m)
	if err2 != nil {
		log.Fatal(err2)
		return
	}

	dp, err3 := NewDataProcessor(cfg, mon, m, da, chDBS)
	if err3 != nil {
		log.Fatal(err3)
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go chDBS.StartDBCommitTask()
	time.Sleep(time.Duration(cfg.General.Sleepinterval) * time.Millisecond)

	wg.Add(1)
	go da.StartDataAdapterTask()
	time.Sleep(time.Duration(cfg.General.Sleepinterval) * time.Millisecond)

	wg.Add(1)
	go dp.StartDataProcessTaskWithAdapter()
	time.Sleep(time.Duration(cfg.General.Sleepinterval) * time.Millisecond)

	if cfg.PipeTopic.Enablegroupnum {
		for idx := cfg.PipeTopic.Begingroupnum; idx <= cfg.PipeTopic.Endgroupnum; idx++ {
			wg.Add(1)
			time.Sleep(time.Duration(cfg.PipeInfo.Taskinterval) * time.Millisecond)
			pp := NewPipe(cfg, idx, mon, m, &dp.DataChan)
			go pp.Start()
		}
	} else {
		wg.Add(1)
		pp := NewPipe(cfg, 0, mon, m, &dp.DataChan)
		go pp.Start()
	}

	log.Infof("%s", m.GetMetricsInfo("Metrics Information"))
	wg.Wait()
}

func main() {
	app := cli.NewApp()
	app.Name = "json message pump"
	app.Version = "21.01.20"
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{
			Name: "Abe.Chua",
		},
	}
	app.Usage = "A command-line simulator for generating messages published to the MQTT broker."

	app.Commands = []cli.Command{
		{
			Name:   "run",
			Usage:  "pump the json-style messages.",
			Action: runPump,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config, c",
					Usage: "config file path",
					Value: "~/.pump-plus.ini",
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
					Value: "~/.pump-plus.ini",
				},
			},
		},
		{
			Name:   "check",
			Usage:  "Check the configuration information for correct data adapter operation.",
			Action: checkDataAdapterConfiguration,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config, c",
					Usage: "config file path",
					Value: "~/.pump-plus.ini",
				},
			},
		},
	}

	_ = app.Run(os.Args)
}
