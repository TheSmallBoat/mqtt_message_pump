package main

import (
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type MqttPubSubClient struct {
	MqttClient MQTT.Client
	Opts       *MQTT.ClientOptions
	Topic      string
	Qos        uint8
	PipeChan   *chan bool
	MsgChan    *chan Message
}

func newMqttOptions(cfg *Config, useMonitor bool) *MQTT.ClientOptions {
	if cfg.General.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	if useMonitor {
		var brokerUri = fmt.Sprintf("%s://%s:%d", cfg.MonitorMqtt.Scheme, cfg.MonitorMqtt.Hostname, cfg.MonitorMqtt.Port)
		log.Infof("Monitor Broker URI: %s", brokerUri)
		return initMqttOptions(brokerUri, cfg.MonitorMqtt.Username, cfg.MonitorMqtt.Password, cfg.MonitorMqtt.Cleansession, cfg.MonitorMqtt.Pingtimeout, cfg.MonitorMqtt.Keepalive)
	} else {
		var brokerUri = fmt.Sprintf("%s://%s:%d", cfg.SourceMqtt.Scheme, cfg.SourceMqtt.Hostname, cfg.SourceMqtt.Port)
		return initMqttOptions(brokerUri, cfg.SourceMqtt.Username, cfg.SourceMqtt.Password, cfg.SourceMqtt.Cleansession, cfg.SourceMqtt.Pingtimeout, cfg.SourceMqtt.Keepalive)
	}
}

func initMqttOptions(brokerUri string, username string, password string, cleansession bool, pingtimeout uint8, keepalive uint16) *MQTT.ClientOptions {
	opts := MQTT.NewClientOptions()

	opts.SetAutoReconnect(true)
	opts.SetCleanSession(cleansession)
	opts.SetPingTimeout(time.Duration(pingtimeout) * time.Second)
	opts.SetConnectTimeout(time.Duration(keepalive) * time.Second)

	opts.AddBroker(brokerUri)
	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}

	return opts
}

// getRandomClientId returns randomized ClientId.
func getRandomClientId(clientName string, maxClientIdLen uint8) string {
	const alphaNum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, maxClientIdLen)
	_, _ = rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphaNum[b%byte(len(alphaNum))]
	}
	return clientName + "-pump-pipe-" + string(bytes)
}

// with Connects connect to the MQTT broker with Options.
func NewPumpMqttClient(cfg *Config, pipeNum uint16, pipeChannel *chan bool, msgChannel *chan Message) *MqttPubSubClient {
	clientId := getRandomClientId(cfg.PipeTopic.Targetname, cfg.PipeInfo.Pipeidmaxlen)

	var subTopic string
	if cfg.PipeTopic.Enablegroupnum {
		subTopic = fmt.Sprintf("%s%s/%d", cfg.SourceMqtt.Topicroot, cfg.PipeTopic.Topicprefix, pipeNum)
	} else {
		subTopic = fmt.Sprintf("%s%s/#", cfg.SourceMqtt.Topicroot, cfg.PipeTopic.Topicprefix)
	}
	log.Infof("Pump [%s], source topic : %s", clientId, subTopic)

	opts := newMqttOptions(cfg, false)
	opts.SetClientID(clientId)
	msc := &MqttPubSubClient{Opts: opts, Topic: subTopic, Qos: cfg.MonitorMqtt.Qos, PipeChan: pipeChannel, MsgChan: msgChannel}
	err := msc.setMqttPubSubClientHandler(msc.PumpOnConnect, msc.PumpConnectionLost)
	if err != nil {
		log.Errorf("pipe mqtt subscribe client init err: %s", err)
		return nil
	}
	return msc
}

func NewMonitorMqttClient(cfg *Config) (*MqttPubSubClient, error) {
	clientId := "pump-mon-" + cfg.PipeTopic.Targetname
	target := strings.ReplaceAll(fmt.Sprintf("%s%s", cfg.SourceMqtt.Topicroot, cfg.PipeTopic.Topicprefix), "/", "_")
	pubTopic := fmt.Sprintf("%s/%s", cfg.MonitorMqtt.Topicroot, target)

	opts := newMqttOptions(cfg, true)
	opts.SetClientID(clientId)
	mpc := &MqttPubSubClient{Opts: opts, Topic: pubTopic, Qos: cfg.MonitorMqtt.Qos, PipeChan: nil}
	err := mpc.setMqttPubSubClientHandler(mpc.MonitorOnConnect, mpc.MonitorConnectionLost)

	log.Infof("monitor: %s, topic: %s, qos: %d ", clientId, mpc.Topic, mpc.Qos)
	return mpc, err
}

func (m *MqttPubSubClient) setMqttPubSubClientHandler(onConn MQTT.OnConnectHandler, conLostHandler MQTT.ConnectionLostHandler) error {
	m.Opts.SetOnConnectHandler(onConn)
	m.Opts.SetConnectionLostHandler(conLostHandler)

	client := MQTT.NewClient(m.Opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	m.MqttClient = client
	return nil
}

func (m *MqttPubSubClient) onMessageReceived(client MQTT.Client, msg MQTT.Message) {
	log.Debugf("topic:%s", msg.Topic())

	messageObject := Message{
		Topic:        msg.Topic(),
		Payload:      msg.Payload(),
		ReceivedTime: time.Now().Unix(),
	}
	*m.MsgChan <- messageObject
}

func (m *MqttPubSubClient) PumpOnConnect(client MQTT.Client) {
	log.Debugf("Pump pipe: [%s] connected to mqtt broker.", m.Opts.ClientID)

	if token := client.Subscribe(m.Topic, byte(m.Qos), m.onMessageReceived); token.Wait() && token.Error() != nil {
		log.Error(token.Error())
		*m.PipeChan <- false
		return
	}
	*m.PipeChan <- true
}

func (m *MqttPubSubClient) PumpConnectionLost(client MQTT.Client, reason error) {
	*m.PipeChan <- false
	log.Debugf("Pump pipe: [%s] has lost its connection to the mqtt broker: %s", m.Opts.ClientID, reason)
}

func (m *MqttPubSubClient) Disconnect() {
	if m.MqttClient.IsConnected() {
		m.MqttClient.Disconnect(20)
		if *m.PipeChan != nil {
			*m.PipeChan <- false
		}
		log.Debugf("[%s] mqtt disconnected", m.Opts.ClientID)
	}
}

func (m *MqttPubSubClient) MonitorOnConnect(client MQTT.Client) {
	log.Debugf("Monitor [%s] mqtt connected", m.Opts.ClientID)
}

func (m *MqttPubSubClient) MonitorConnectionLost(client MQTT.Client, reason error) {
	log.Errorf("Monitor mqtt disconnected: %s", reason)
}
