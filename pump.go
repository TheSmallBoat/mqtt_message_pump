package main

type PipeTopicConf struct {
	Targetname     string
	Topicprefix    string
	Enablegroupnum bool
	Begingroupnum  uint16
	Endgroupnum    uint16
}

type PipeInfoConf struct {
	Pipeidmaxlen uint8
	Taskinterval uint8
	Buffersize   uint
}

type Pipe struct {
	MqttPubSubClient *MqttPubSubClient
	Monitor          *Monitor

	PipeIdNum      uint16
	MsgChan        chan Message
	ClickHouseChan *chan Message
}

type Message struct {
	Topic        string
	Payload      []byte
	ReceivedTime uint32
}

func NewPipe(cfg *Config, pipeNum uint16, mon *Monitor, chc *chan Message) *Pipe {
	mc := make(chan Message, cfg.PipeInfo.Buffersize)
	mpsc := NewPumpMqttClient(cfg, pipeNum, &mon.PipeChan, &mc)
	return &Pipe{
		MqttPubSubClient: mpsc,
		Monitor:          mon,
		PipeIdNum:        pipeNum,
		MsgChan:          mc,
		ClickHouseChan:   chc,
	}
}

func (pp *Pipe) start() {
	for {
		select {
		case msg, ok := <-pp.MsgChan:
			if !ok {
				pp.Monitor.SubChan <- 0
			} else {
				pp.Monitor.SubChan <- uint(len(msg.Payload) + len(msg.Topic))
				*pp.ClickHouseChan <- msg
			}
		}
	}
}
