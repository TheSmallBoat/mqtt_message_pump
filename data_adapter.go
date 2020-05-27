package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/json-iterator/go"
	"reflect"
	"sort"
	"strings"
)

const DataAdapterName = "Multi-Record Cascading Tile Adapter"
const MinDataAdapterChannelBufferSize = uint(8)
const MaxDataAdapterChannelBufferSize = uint(32)

type DataAdapter struct {
	Monitor *Monitor
	Version string

	MessageTableDDL  string
	MessageTableKeys []string

	DataNeedAdapterObject   chan DataNeedAdapterObject
	DataAdapterObjectDBChan *chan DataAdapterDBObject
}

type DataNeedAdapterObject struct {
	Mid          string
	PayLoad      []byte
	ReceivedTime int64 //match the type of time.Now().Unix()
}

func NewDataAdapter(cfg *Config, mon *Monitor, m *Metrics) (*DataAdapter, error) {
	bufferSize := MinDataAdapterChannelBufferSize
	if cfg.AdapterInfo.Buffersize > bufferSize {
		bufferSize = cfg.AdapterInfo.Buffersize
		if bufferSize > MaxDataAdapterChannelBufferSize {
			bufferSize = MaxDataAdapterChannelBufferSize
		}
	}
	m.ChannelBufferSize.DataAdapter = bufferSize

	dap := &DataAdapter{
		Monitor:                 mon,
		Version:                 "2020.01.22",
		MessageTableDDL:         "",
		MessageTableKeys:        nil,
		DataNeedAdapterObject:   make(chan DataNeedAdapterObject, bufferSize),
		DataAdapterObjectDBChan: nil,
	}
	err := dap.checkAdapterName(cfg)
	return dap, err
}

func (da *DataAdapter) checkAdapterName(cfg *Config) error {
	if cfg.AdapterInfo.Adapter == DataAdapterName {
		log.Infof("The data adapter name is '%s, Version: %s', that is supported by this pump program ... \n ", DataAdapterName, da.Version)
	} else {
		log.Infof("The data adapter name is '%s', that is not supported by this pump program ... please check it ... \n ", cfg.AdapterInfo.Adapter)
		return fmt.Errorf("not be supportted data adapter error: %s", cfg.AdapterInfo.Adapter)
	}
	return nil
}

func isMapType(item interface{}) bool {
	switch item.(type) {
	case map[string]interface{}:
		return true
	}
	return false
}

func getItemFromMap(mp *map[string]interface{}, key string, item interface{}) {
	switch item.(type) {
	case map[string]interface{}:
		for k, v := range item.(map[string]interface{}) {
			var newKey string
			if key != "" {
				newKey = fmt.Sprintf("%s_%s", key, k)
			} else {
				newKey = k
			}
			getItemFromMap(mp, newKey, v)
		}
		return
	}
	(*mp)[key] = item
}

func (da *DataAdapter) processMapObject(m *map[string]interface{}, isCheck bool) (*[]map[string]interface{}, error) {
	dataItems := make([]map[string]interface{}, 0, len(*m))
	for ik, iv := range *m {
		if isMapType(iv) {
			mp := map[string]interface{}{}
			mp["key_id"] = ik
			getItemFromMap(&mp, "", iv)
			dataItems = append(dataItems, mp)

			if isCheck {
				log.Infof("The item information ... ... \n KeyId: %+v, Item: %+v ", ik, iv)
				log.Info("Expanded parsed content ... ")
				keys := make([]string, 0, len(mp))
				for mpk := range mp {
					keys = append(keys, mpk)
				}
				sort.Strings(keys)

				ddl := ""
				for _, mpk := range keys {
					log.Infof("Key: %+v, Value: %+v, Type: %+v", mpk, mp[mpk], reflect.TypeOf(mp[mpk]))
					ddl += fmt.Sprintf(", %s %s", mpk, strings.Title(reflect.TypeOf(mp[mpk]).String()))
				}

				if da.MessageTableDDL == "" {
					da.MessageTableDDL = ddl
					da.MessageTableKeys = keys
					log.Infof("Get The Adapter Message Table Core DDL Parts ... \n %s \n ", ddl)
				} else {
					if da.MessageTableDDL == ddl {
						log.Infof("Get The Adapter Message Table Core DDL Parts  ... [Equal] \n %s \n ", ddl)
					} else {
						log.Warnf("Get The Adapter Message Table Core DDL Parts  ... [Not Equal] ... Please Check it ...  \n %s \n %s \n ", ddl, da.MessageTableDDL)
					}
				}
			}
		} else {
			if isCheck {
				log.Fatal("The json object is not match the 'Multi-Record Cascading Tile Adapter', please check the protocol ... \n ")
			}
			return nil, fmt.Errorf("adapter protocol match error")
		}
	}
	return &dataItems, nil
}

func (da *DataAdapter) ProcessDataObject(msg []byte, isCheck bool) (*[]map[string]interface{}, error) {
	m := map[string]interface{}{}

	err1 := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(msg, &m)
	if err1 != nil {
		return nil, err1
	}

	items, err2 := da.processMapObject(&m, isCheck)
	// if isCheck false,need to add monitor section
	return items, err2
}

func (da *DataAdapter) CheckJsonSample(sample string) *[]map[string]interface{} {
	log.Infof("Prepare to process the json-style sample from configuration file ... \n%s ", sample)

	items, err := da.ProcessDataObject([]byte(sample), true)
	if err != nil {
		log.Fatal(err)
	}
	return items
}

func (da *DataAdapter) StartDataAdapterTask() {
	for {
		select {
		case dnao, ok := <-da.DataNeedAdapterObject:
			if !ok {
				da.Monitor.DataAdapterChan <- 0
			} else {
				items, err := da.ProcessDataObject(dnao.PayLoad, false)
				if err != nil {
					da.Monitor.DataAdapterPDOChan <- 0
				} else {
					*da.DataAdapterObjectDBChan <- DataAdapterDBObject{dnao.Mid, *items, dnao.ReceivedTime}
					da.Monitor.DataAdapterPDOChan <- 1
				}
				da.Monitor.DataAdapterChan <- uint32(len(dnao.PayLoad) + len(dnao.Mid) + 4)
			}
		}
	}
}
