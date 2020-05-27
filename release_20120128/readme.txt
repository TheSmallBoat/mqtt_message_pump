
# configuration file need to add the JSON-style sample of the message for getting from the MQTT broker
jsonsample = "{\"1\":{\"command\":\"property.publish\",\"params\":{\"thingKey\":\"545420B443E8455335363335520C6E6B\",\"ts\":\"2020-01-05T20:31:00Z\",\"key\":\"ut\",\"value\":564}},\"2\":{\"command\":\"property.publish\",\"params\":{\"thingKey\":\"545420B443E8455335363335520C7777\",\"ts\":\"2020-01-06T21:31:00Z\",\"key\":\"ut\",\"value\":128}}}"

Based on this sample information, the program will automatically parse and generate the relevant database table structure, and perform data insertion actions.
The first level supports multiple data records, and the other subsets support multiple levels of nested information structures.
The database field corresponds to the sample Key value and supports free addition.
At the same time, the program can roughly determine the type of the data field.


./pump-plus % ./pump-plus check -c ./conf/pump-plus.ini.example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
INFO[2020-01-22T08:13:02+13:00] Loading configuration information from './conf/pump-plus.ini.example'
INFO[2020-01-22T08:13:02+13:00] Configuration information ...
 [general] => {Debug:true Sleepinterval:100}
 [source-mqtt] => {Scheme:tcp Hostname:127.0.0.1 Port:1883 Cleansession:true Qos:1 Pingtimeout:1 Keepalive:60 Username:X Password: Topicroot:*Regular*/JSON_Simulator/SimTest}
 [pipe-topic] => {Targetname:ST Topicprefix:/NZ/TestZone/TT Enablegroupnum:true Begingroupnum:0 Endgroupnum:4}
 [clickhouse] => {Scheme:tcp Hostname:127.0.0.1 Port:19000 Username: Password: Database:SimTest Compress:true Debug:false}
 [monitor-mqtt] => {Scheme:tcp Hostname:127.0.0.1 Port:1883 Cleansession:false Qos:0 Pingtimeout:1 Keepalive:60 Username:X Password: Topicroot:*Special*/Monitor/Json_Pump}
 [monitor-info] => {Buffersize:32 PublishInterval:5}
 [pipe-info] => {Pipeidmaxlen:5 Taskinterval:50 Buffersize:32}
 [processor-info] => {Buffersize:32}
 [adapter-info] => {Adapter:Multi-Record Cascading Tile Adapter Jsonsample:{"1":{"command":"property.publish","params":{"thingKey":"545420B443E8455335363335520C6E6B","ts":"2020-01-05T20:31:00Z","key":"ut","value":564}},"2":{"command":"property.publish","params":{"thingKey":"545420B443E8455335363335520C7777","ts":"2020-01-06T21:31:00Z","key":"ut","value":128}}}}

DEBU[2020-01-22T08:13:02+13:00] The debug mode ... [ENABLE]
INFO[2020-01-22T08:13:02+13:00] The data adapter name is 'Multi-Record Cascading Tile Adapter, Version: 2020.01.22', that is supported by this pump program ...

INFO[2020-01-22T08:13:02+13:00] Prepare to process the json-style sample from configuration file ...
{"1":{"command":"property.publish","params":{"thingKey":"545420B443E8455335363335520C6E6B","ts":"2020-01-05T20:31:00Z","key":"ut","value":564}},"2":{"command":"property.publish","params":{"thingKey":"545420B443E8455335363335520C7777","ts":"2020-01-06T21:31:00Z","key":"ut","value":128}}}
INFO[2020-01-22T08:13:02+13:00] The item information ... ...
 KeyId: 1, Item: map[command:property.publish params:map[key:ut thingKey:545420B443E8455335363335520C6E6B ts:2020-01-05T20:31:00Z value:564]]
INFO[2020-01-22T08:13:02+13:00] Expanded parsed content ...
INFO[2020-01-22T08:13:02+13:00] Key: command, Value: property.publish, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: key_id, Value: 1, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: params_key, Value: ut, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: params_thingKey, Value: 545420B443E8455335363335520C6E6B, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: params_ts, Value: 2020-01-05T20:31:00Z, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: params_value, Value: 564, Type: float64
INFO[2020-01-22T08:13:02+13:00] Get The Adapter Message Table Core DDL Parts ...
 , command String, key_id String, params_key String, params_thingKey String, params_ts String, params_value Float64

INFO[2020-01-22T08:13:02+13:00] The item information ... ...
 KeyId: 2, Item: map[command:property.publish params:map[key:ut thingKey:545420B443E8455335363335520C7777 ts:2020-01-06T21:31:00Z value:128]]
INFO[2020-01-22T08:13:02+13:00] Expanded parsed content ...
INFO[2020-01-22T08:13:02+13:00] Key: command, Value: property.publish, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: key_id, Value: 2, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: params_key, Value: ut, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: params_thingKey, Value: 545420B443E8455335363335520C7777, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: params_ts, Value: 2020-01-06T21:31:00Z, Type: string
INFO[2020-01-22T08:13:02+13:00] Key: params_value, Value: 128, Type: float64
INFO[2020-01-22T08:13:02+13:00] Get The Adapter Message Table Core DDL Parts  ... [Equal]
 , command String, key_id String, params_key String, params_thingKey String, params_ts String, params_value Float64

INFO[2020-01-22T08:13:02+13:00] ClickHouse DB Store ... DSN: tcp://127.0.0.1:19000?username=&debug=false&compress=true
INFO[2020-01-22T08:13:02+13:00] Connect to db (tcp://127.0.0.1:19000?username=&debug=false&compress=true) ... [ok]
INFO[2020-01-22T08:13:02+13:00] Execute the DDL ... CREATE DATABASE IF NOT EXISTS SimTest
INFO[2020-01-22T08:13:02+13:00] Execute the DDL ... CREATE TABLE IF NOT EXISTS SimTest.raw_message (
                mid String COMMENT 'message id',
                topic String COMMENT 'message topic',
                payload String COMMENT 'message payloads',
                collect_datetime DateTime COMMENT 'Date and time of collecting data from the message hub',
                collect_date Date MATERIALIZED toDate(collect_datetime) COMMENT 'date of collecting data from the message hub'
        ) ENGINE = MergeTree(collect_date, (mid, topic, collect_date), 8192)
DEBU[2020-01-22T08:13:02+13:00] Adapter Message Table DDL ... CREATE TABLE IF NOT EXISTS SimTest.json_message (
                mid String, command String, key_id String, params_key String, params_thingKey String, params_ts String, params_value Float64,
                collect_datetime DateTime,
                collect_date Date MATERIALIZED toDate(collect_datetime)
        ) ENGINE = MergeTree(collect_date, (mid, collect_date), 8192)
INFO[2020-01-22T08:13:02+13:00] Execute the DDL ... CREATE TABLE IF NOT EXISTS SimTest.json_message (
                mid String, command String, key_id String, params_key String, params_thingKey String, params_ts String, params_value Float64,
                collect_datetime DateTime,
                collect_date Date MATERIALIZED toDate(collect_datetime)
        ) ENGINE = MergeTree(collect_date, (mid, collect_date), 8192)
INFO[2020-01-22T08:13:02+13:00] Execute the DDL ... CREATE TABLE IF NOT EXISTS SimTest.raw_message_check (
                mid String, topic String, payload String, collect_datetime DateTime) ENGINE = Memory
INFO[2020-01-22T08:13:02+13:00] Do Insert Raw Message Table For Check ... ... [Done]
INFO[2020-01-22T08:13:02+13:00] Query Raw Message Table For Check ... ...
INFO[2020-01-22T08:13:02+13:00] Query SimTest.raw_message_check record ... ...
 mid: message_check, topic: topic_check, payload: {"1":{"command":"property.publish","params":{"thingKey":"545420B443E8455335363335520C6E6B","ts":"2020-01-05T20:31:00Z","key":"ut","value":564}},"2":{"command":"property.publish","params":{"thingKey":"545420B443E8455335363335520C7777","ts":"2020-01-06T21:31:00Z","key":"ut","value":128}}}, collect_datetime: 2020-01-21 19:13:02 +0000 UTC
INFO[2020-01-22T08:13:02+13:00] Do Query Raw Message Table For Check ... ... [Done]
DEBU[2020-01-22T08:13:02+13:00] Adapter Message Table DDL ... CREATE TABLE IF NOT EXISTS SimTest.json_message_check (
                mid String, command String, key_id String, params_key String, params_thingKey String, params_ts String, params_value Float64, collect_datetime DateTime) ENGINE = Memory
INFO[2020-01-22T08:13:02+13:00] Execute the DDL ... CREATE TABLE IF NOT EXISTS SimTest.json_message_check (
                mid String, command String, key_id String, params_key String, params_thingKey String, params_ts String, params_value Float64, collect_datetime DateTime) ENGINE = Memory
DEBU[2020-01-22T08:13:02+13:00] Do Insert Adapter Message Table For Check ... Insert SQL ...
 INSERT INTO SimTest.json_message_check (mid , params_ts, params_key, params_value, key_id, command, params_thingKey, collect_datetime) VALUES ('message_check', '2020-01-05T20:31:00Z', 'ut', 564, '1', 'property.publish', '545420B443E8455335363335520C6E6B', 1579633982)
DEBU[2020-01-22T08:13:02+13:00] Do Insert Adapter Message Table For Check ... Insert SQL ...
 INSERT INTO SimTest.json_message_check (mid , params_value, params_thingKey, params_ts, key_id, command, params_key, collect_datetime) VALUES ('message_check', 128, '545420B443E8455335363335520C7777', '2020-01-06T21:31:00Z', '2', 'property.publish', 'ut', 1579633982)
