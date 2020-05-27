package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"awesomeProject/iotplatform/pump-plus/deque"

	log "github.com/Sirupsen/logrus"
	"github.com/kshvakov/clickhouse"
)

const (
	MinDataObjectStoreChannelBufferSize = uint(8)
	MaxDataObjectStoreChannelBufferSize = uint(32)
	DBObjectItemsSize                   = int(64)
	DBCommitInterval                    = 5
	DBCommitCheckTaskSleepInterval      = 1
)

type ClickHouseDBStore struct {
	Monitor   *Monitor
	DBConnect *sql.DB

	DataRawObjectDBChan     chan DataRawDBObject
	DataAdapterObjectDBChan chan DataAdapterDBObject

	DataSourceName          string
	RawMessageTableName     string
	AdapterMessageTableName string
	AdapterMessageTableDDL  string
	AdapterMessageTableKeys *[]string

	InsertRawObjectSQL             string
	InsertRawObjectForCheckSQL     string
	InsertAdapterObjectSQL         string
	InsertAdapterObjectForCheckSQL string

	RawDbObjectQueue     deque.Deque
	AdapterDbObjectQueue deque.Deque
}

type DataRawDBObject struct {
	Mid     string
	Message Message
}

type DataAdapterDBObject struct {
	Mid          string
	Items        []map[string]interface{}
	ReceivedTime int64
}

func getInsertAdapterSQL(adapterMessageTableKeys *[]string, tableName string, isCheck bool) string {
	strValues := ""
	strKeys := strings.Join(*adapterMessageTableKeys, ",")
	for i := 0; i < len(*adapterMessageTableKeys); i++ {
		strValues += ",?"
	}

	check := ""
	if isCheck {
		check = "_check"
	}
	sqlStr := fmt.Sprintf("INSERT INTO %s%s (mid,%s,collect_datetime) VALUES (?%s,?)", tableName, check, strKeys, strValues)
	return sqlStr
}
func NewClickHouseDBStore(cfg *Config, mon *Monitor, m *Metrics, theAdapterMessageTableDDL string, theAdapterMessageTableKeys *[]string) (*ClickHouseDBStore, error) {
	var rawTableName, adapterTableName string

	if cfg.AdapterInfo.Rawtablename != "" {
		rawTableName = fmt.Sprintf("%s.%s", cfg.ClickHouse.Database, cfg.AdapterInfo.Rawtablename)
	} else {
		rawTableName = fmt.Sprintf("%s.Raw_Message", cfg.ClickHouse.Database)
	}

	if cfg.AdapterInfo.Adaptertablename != "" {
		adapterTableName = fmt.Sprintf("%s.%s", cfg.ClickHouse.Database, cfg.AdapterInfo.Adaptertablename)
	} else {
		adapterTableName = fmt.Sprintf("%s.Json_Message", cfg.ClickHouse.Database)
	}

	dsn := fmt.Sprintf("%s://%s:%d?username=%s&debug=%t&compress=%t", cfg.ClickHouse.Scheme, cfg.ClickHouse.Hostname, cfg.ClickHouse.Port, cfg.ClickHouse.Username, cfg.ClickHouse.Debug, cfg.ClickHouse.Compress)
	log.Infof("ClickHouse DB Store ... Data Source Name: %s ", dsn)

	bufferSize := MinDataObjectStoreChannelBufferSize
	if cfg.DbStoreInfo.Buffersize > bufferSize {
		bufferSize = cfg.DbStoreInfo.Buffersize
		if bufferSize > MaxDataObjectStoreChannelBufferSize {
			bufferSize = MaxDataObjectStoreChannelBufferSize
		}
	}
	m.ChannelBufferSize.DataObjectStore = bufferSize

	var rawDbObjectDeque, adapterDbObjectDeque deque.Deque

	chDBS := &ClickHouseDBStore{
		Monitor:                        mon,
		DBConnect:                      nil,
		DataRawObjectDBChan:            make(chan DataRawDBObject, bufferSize),
		DataAdapterObjectDBChan:        make(chan DataAdapterDBObject, bufferSize),
		RawMessageTableName:            rawTableName,
		AdapterMessageTableName:        adapterTableName,
		DataSourceName:                 dsn,
		AdapterMessageTableDDL:         theAdapterMessageTableDDL,
		AdapterMessageTableKeys:        theAdapterMessageTableKeys,
		InsertRawObjectSQL:             fmt.Sprintf("INSERT INTO %s (mid, topic, payload, collect_datetime) VALUES (?, ?, ?, ?)", rawTableName),
		InsertRawObjectForCheckSQL:     fmt.Sprintf("INSERT INTO %s_check (mid, topic, payload, collect_datetime) VALUES (?, ?, ?, ?)", rawTableName),
		InsertAdapterObjectSQL:         getInsertAdapterSQL(theAdapterMessageTableKeys, adapterTableName, false),
		InsertAdapterObjectForCheckSQL: getInsertAdapterSQL(theAdapterMessageTableKeys, adapterTableName, true),
		RawDbObjectQueue:               rawDbObjectDeque,
		AdapterDbObjectQueue:           adapterDbObjectDeque,
	}

	err := chDBS.connect()
	if err != nil {
		return nil, err
	}

	dbDDL := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s `, cfg.ClickHouse.Database)
	err = chDBS.execDDL(dbDDL)
	if err != nil {
		return chDBS, err
	}
	rtbDDL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		mid String COMMENT 'message id',
		topic String COMMENT 'message topic',
		payload String COMMENT 'message payloads',
  		collect_datetime DateTime COMMENT 'Date and time of collecting data from the message hub',
  		collect_date Date MATERIALIZED toDate(collect_datetime) COMMENT 'date of collecting data from the message hub'
	) ENGINE = MergeTree(collect_date, (mid, topic, collect_date), 8192)`, chDBS.RawMessageTableName)
	err = chDBS.execDDL(rtbDDL)
	if err != nil {
		return chDBS, err
	}
	atbDDL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		mid String%s,
		collect_datetime DateTime,
		collect_date Date MATERIALIZED toDate(collect_datetime)
	) ENGINE = MergeTree(collect_date, (mid, collect_date), 8192)`, chDBS.AdapterMessageTableName, chDBS.AdapterMessageTableDDL)
	log.Debugf("Adapter Message Table DDL ... %s ", atbDDL)

	err = chDBS.execDDL(atbDDL)

	return chDBS, err
}

func (chDBS *ClickHouseDBStore) checkError(err error) {
	if err != nil {
		chDBS.Monitor.DBStoreErrChan <- true
		log.Errorf("data base store error: %s", err)
	}
}

func (chDBS *ClickHouseDBStore) connect() error {
	connect, err := sql.Open("clickhouse", chDBS.DataSourceName)
	chDBS.checkError(err)
	if err != nil {
		return err
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Infof("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Errorf("clickhouse db connect error: %s", err)
		}
		return err
	}
	chDBS.DBConnect = connect
	log.Infof("Connect to db (%s) ... [ok]", chDBS.DataSourceName)
	return nil
}

func (chDBS *ClickHouseDBStore) execDDL(ddl string) error {
	if chDBS.DBConnect != nil {
		_, err := chDBS.DBConnect.Exec(ddl)
		chDBS.checkError(err)

		if err == nil {
			log.Infof("Execute the DDL ... %s ", ddl)
		}
		return err
	} else {
		return fmt.Errorf("execute ddl error due to non connect pointer")
	}
}

func (chDBS *ClickHouseDBStore) prepare(sql string) (*sql.Tx, *sql.Stmt, error, error) {
	tx, err1 := chDBS.DBConnect.Begin()
	chDBS.checkError(err1)

	stmt, err2 := tx.Prepare(sql)
	chDBS.checkError(err2)

	return tx, stmt, err1, err2
}

func (chDBS *ClickHouseDBStore) DoInsertRawMessageTableForCheck(samplePayLoad string) error {
	tbddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_check (
		mid String, topic String, payload String, collect_datetime DateTime) ENGINE = Memory`, chDBS.RawMessageTableName)
	err := chDBS.execDDL(tbddl)
	if err != nil {
		chDBS.checkError(err)
		return err
	}

	log.Debugf("Do Insert The Adapter Message Table For Check ... Prepare Insert SQL ... \n %s ", chDBS.InsertRawObjectForCheckSQL)
	tx, stmt, err1, err2 := chDBS.prepare(chDBS.InsertRawObjectForCheckSQL)
	if err1 == nil && err2 == nil {
		_, err3 := stmt.Exec("message_check", "topic_check", samplePayLoad, time.Now().Unix())
		chDBS.checkError(err3)
		err4 := tx.Commit()
		chDBS.checkError(err4)
		if err3 == nil && err4 == nil {
			log.Infof("Do Insert The Raw Message Table For Check ... ... [Done] ")
			return nil
		}
		return fmt.Errorf("do insert raw message table for check error : %s,%s", err3, err4)
	} else {
		return fmt.Errorf("do insert raw message table for check error : %s,%s", err1, err2)
	}
}

func (chDBS *ClickHouseDBStore) DoQueryRawMessageTableForCheck() error {
	log.Infof("Prepare To Query The Raw Message Table For Check ... ... ")

	querySQL := fmt.Sprintf("SELECT mid, topic, payload, collect_datetime FROM %s_check", chDBS.RawMessageTableName)
	log.Debug(querySQL)

	rows, err1 := chDBS.DBConnect.Query(querySQL)
	chDBS.checkError(err1)

	for rows.Next() {
		var (
			mid, topic, payload string
			collectDatetime     time.Time
		)
		chDBS.checkError(rows.Scan(&mid, &topic, &payload, &collectDatetime))
		log.Infof("Query %s_check record ... ... \n mid: %s, topic: %s, payload: %s, collect_datetime: %s", chDBS.RawMessageTableName, mid, topic, payload, collectDatetime)
	}

	log.Infof("Prepare To Drop The Raw Message Table For Check ... ... ")
	dropSQL := fmt.Sprintf("DROP TABLE %s_check ", chDBS.RawMessageTableName)
	log.Debug(dropSQL)

	_, err2 := chDBS.DBConnect.Exec(dropSQL)
	chDBS.checkError(err2)

	if err1 == nil && err2 == nil {
		log.Infof("Do Query The Raw Message Table For Check ... ... [Done] ")
		return nil
	} else {
		return fmt.Errorf("do query the raw message table for check error : %s,%s", err1, err2)
	}
}

func (chDBS *ClickHouseDBStore) DoInsertAdapterMessageTableForCheck(items []map[string]interface{}) error {
	atbDDL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_check (
		mid String%s, collect_datetime DateTime) ENGINE = Memory`, chDBS.AdapterMessageTableName, chDBS.AdapterMessageTableDDL)
	log.Debugf("Adapter Message Table DDL ... %s ", atbDDL)

	err := chDBS.execDDL(atbDDL)
	if err != nil {
		chDBS.checkError(err)
	}

	log.Debugf("Do Insert The Adapter Message Table For Check ... Prepare Insert SQL ... \n %s ", chDBS.InsertAdapterObjectForCheckSQL)
	tx, stmt, err1, err2 := chDBS.prepare(chDBS.InsertAdapterObjectForCheckSQL)
	if err1 == nil && err2 == nil {
		for _, mp := range items {
			arr := make([]interface{}, 0, len(*chDBS.AdapterMessageTableKeys)+2)
			arr = append(arr, "message_check")
			for _, key := range *chDBS.AdapterMessageTableKeys {
				arr = append(arr, mp[key])
			}
			arr = append(arr, time.Now().Unix())

			_, err := stmt.Exec(arr...)
			chDBS.checkError(err)
		}

		err := tx.Commit()
		chDBS.checkError(err)
		if err == nil {
			log.Infof("Do Insert Raw Message Table For Check ... ... [Done] ")
			return nil
		}
		return fmt.Errorf("do insert the adapter message table for check error : %s", err)
	} else {
		return fmt.Errorf("do insert the adapter message table for check error : %s,%s", err1, err2)
	}
}

func (chDBS *ClickHouseDBStore) DoQueryAdapterMessageTableForCheck() error {
	log.Infof("Prepare To Query The Adapter Message Table For Check ... ... ")

	querySQL := fmt.Sprintf("SELECT count() as total FROM %s_check", chDBS.AdapterMessageTableName)
	log.Debug(querySQL)

	rows, err1 := chDBS.DBConnect.Query(querySQL)
	chDBS.checkError(err1)

	for rows.Next() {
		var total int
		chDBS.checkError(rows.Scan(&total))
		log.Infof("Query %s_check records ... found [%d] items ... ", chDBS.AdapterMessageTableName, total)
	}

	log.Infof("Prepare To Drop The Raw Message Table For Check ... ... ")
	dropSQL := fmt.Sprintf("DROP TABLE %s_check ", chDBS.AdapterMessageTableName)
	log.Debug(dropSQL)

	_, err2 := chDBS.DBConnect.Exec(dropSQL)
	chDBS.checkError(err2)

	if err1 == nil && err2 == nil {
		log.Infof("Do Query The Adapter Message Table For Check ... ... [Done] ")
		return nil
	} else {
		return fmt.Errorf("do query the adapter message table for check error : %s,%s", err1, err2)
	}
}

func (chDBS *ClickHouseDBStore) dBStoreRawDataObjectCommit(itemsNum int, isFailed bool) {
	for i := 0; i < itemsNum; i++ {
		if isFailed {
			chDBS.Monitor.DBStoreRawDataObjectCommitChan <- 0
		} else {
			chDBS.Monitor.DBStoreRawDataObjectCommitChan <- 1
		}
	}
}

func (chDBS *ClickHouseDBStore) doInsertRawDBObject() {
	size := chDBS.RawDbObjectQueue.Len()
	if size > 0 {
		tx, stmt, err1, err2 := chDBS.prepare(chDBS.InsertRawObjectSQL)
		if err1 == nil && err2 == nil {
			var errNum = 0
			for i := 0; i < size; i++ {
				rdo := chDBS.RawDbObjectQueue.PopFront().(*DataRawDBObject)
				_, err := stmt.Exec(rdo.Mid, rdo.Message.Topic, string(rdo.Message.Payload), rdo.Message.ReceivedTime)
				chDBS.checkError(err)
				if err != nil {
					errNum++
				}
			}
			err := tx.Commit()
			chDBS.checkError(err)
			if err == nil {
				if errNum > 0 {
					chDBS.dBStoreRawDataObjectCommit(errNum, true)
					chDBS.dBStoreRawDataObjectCommit(size-errNum, false)
				} else {
					chDBS.dBStoreRawDataObjectCommit(size, false)
				}
			} else {
				chDBS.dBStoreRawDataObjectCommit(size, true)
			}
		} else {
			chDBS.dBStoreRawDataObjectCommit(size, true)
		}
	}
}

func (chDBS *ClickHouseDBStore) dBStoreAdapterDataObjectCommit(itemsNum int, isFailed bool) {
	for i := 0; i < itemsNum; i++ {
		if isFailed {
			chDBS.Monitor.DBStoreAdapterDataObjectCommitChan <- 0
		} else {
			chDBS.Monitor.DBStoreAdapterDataObjectCommitChan <- 1
		}
	}
}

func (chDBS *ClickHouseDBStore) doInsertAdapterDBObject() {
	size := chDBS.AdapterDbObjectQueue.Len()
	if size > 0 {
		tx, stmt, err1, err2 := chDBS.prepare(chDBS.InsertAdapterObjectSQL)
		if err1 == nil && err2 == nil {
			var errNum = 0
			for i := 0; i < size; i++ {
				var errItemNum = 0
				ado := chDBS.AdapterDbObjectQueue.PopFront().(*DataAdapterDBObject)
				for _, mp := range ado.Items {
					arr := make([]interface{}, 0, len(*chDBS.AdapterMessageTableKeys)+2)
					arr = append(arr, ado.Mid)
					for _, key := range *chDBS.AdapterMessageTableKeys {
						arr = append(arr, mp[key])
					}
					arr = append(arr, ado.ReceivedTime)

					_, err := stmt.Exec(arr...)
					chDBS.checkError(err)
					if err != nil {
						errItemNum++
					}
				}
				if errItemNum > 0 {
					errNum++
				}
			}
			err := tx.Commit()
			chDBS.checkError(err)
			if err == nil {
				if errNum > 0 {
					chDBS.dBStoreAdapterDataObjectCommit(errNum, true)
					chDBS.dBStoreAdapterDataObjectCommit(size-errNum, false)
				} else {
					chDBS.dBStoreAdapterDataObjectCommit(size, false)
				}
			} else {
				chDBS.dBStoreAdapterDataObjectCommit(size, true)
			}
		} else {
			chDBS.dBStoreAdapterDataObjectCommit(size, true)
		}
	}
}

func (chDBS *ClickHouseDBStore) StartDBCommitTask() {
	go chDBS.startRawObjectProcessTask()
	go chDBS.startAdapterObjectProcessTask()
	go chDBS.startRawObjectInsertDBTask()
	go chDBS.startAdapterObjectInsertDBTask()
}

func (chDBS *ClickHouseDBStore) startRawObjectInsertDBTask() {
	lastTime := time.Now()
	for {
		if time.Since(lastTime).Seconds() > DBCommitInterval || chDBS.RawDbObjectQueue.Len() > DBObjectItemsSize {
			chDBS.doInsertRawDBObject()
			lastTime = time.Now()
		} else {
			time.Sleep(time.Duration(DBCommitCheckTaskSleepInterval))
		}
	}
}

func (chDBS *ClickHouseDBStore) startAdapterObjectInsertDBTask() {
	lastTime := time.Now()
	for {
		if time.Since(lastTime).Seconds() > DBCommitInterval || chDBS.AdapterDbObjectQueue.Len() > DBObjectItemsSize {
			chDBS.doInsertAdapterDBObject()
			lastTime = time.Now()
		} else {
			time.Sleep(time.Duration(DBCommitCheckTaskSleepInterval))
		}
	}
}

func (chDBS *ClickHouseDBStore) startRawObjectProcessTask() {
	for {
		select {
		case dro, ok := <-chDBS.DataRawObjectDBChan:
			if !ok {
				chDBS.Monitor.DBStoreRawDataObjectReceiveChan <- false
			} else {
				chDBS.RawDbObjectQueue.PushBack(&dro)
				chDBS.Monitor.DBStoreRawDataObjectReceiveChan <- true
			}
		}
	}
}

func (chDBS *ClickHouseDBStore) startAdapterObjectProcessTask() {
	for {
		select {
		case dao, ok := <-chDBS.DataAdapterObjectDBChan:
			if !ok {
				chDBS.Monitor.DBStoreAdapterDataObjectReceiveChan <- false
			} else {
				chDBS.AdapterDbObjectQueue.PushBack(&dao)
				chDBS.Monitor.DBStoreAdapterDataObjectReceiveChan <- true
			}
		}
	}
}
