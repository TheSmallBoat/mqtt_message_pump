package main

const MinPipeDataChannelBufferSize = uint(8)
const MaxPipeDataChannelBufferSize = uint(64)

type Pipe struct {
	MqttPubSubClient *MqttPubSubClient
	Monitor          *Monitor

	PipeIdNum             uint16
	MsgChan               chan Message
	DataProcessorDataChan *chan Message
}

type Message struct {
	Topic        string
	Payload      []byte
	ReceivedTime int64 //match the type of time.Now().Unix()
}

func NewPipe(cfg *Config, pipeNum uint16, mon *Monitor, m *Metrics, dpDataChan *chan Message) *Pipe {
	bufferSize := MinPipeDataChannelBufferSize
	if cfg.PipeInfo.Buffersize > bufferSize {
		bufferSize = cfg.PipeInfo.Buffersize
		if bufferSize > MaxPipeDataChannelBufferSize {
			bufferSize = MaxPipeDataChannelBufferSize
		}
	}
	m.ChannelBufferSize.Pipe = bufferSize

	mChan := make(chan Message, bufferSize)
	mpsc := NewPumpMqttClient(cfg, pipeNum, &mon.PipeChan, &mChan)
	return &Pipe{
		MqttPubSubClient:      mpsc,
		Monitor:               mon,
		PipeIdNum:             pipeNum,
		MsgChan:               mChan,
		DataProcessorDataChan: dpDataChan,
	}
}

func (pp *Pipe) Start() {
	for {
		select {
		case msg, ok := <-pp.MsgChan:
			if !ok {
				pp.Monitor.SubChan <- 0
			} else {
				*pp.DataProcessorDataChan <- msg
				pp.Monitor.SubChan <- uint32(len(msg.Topic) + len(msg.Payload) + 4)
			}
		}
	}
}
