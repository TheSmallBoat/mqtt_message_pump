package main

import "fmt"

type Metrics struct {
	ChannelBufferSize *ChannelBufferSizeMetrics
}

type ChannelBufferSizeMetrics struct {
	Monitor         uint
	Pipe            uint
	DataProcessor   uint
	DataAdapter     uint
	DataObjectStore uint
}

func NewMetrics() *Metrics {
	defaultChannelBufferSizeMetrics := &ChannelBufferSizeMetrics{
		MinMonitorChannelBufferSize,
		MinPipeDataChannelBufferSize,
		MinDataProcessorChannelBufferSize,
		MinDataAdapterChannelBufferSize,
		MinDataObjectStoreChannelBufferSize,
	}
	defaultMetrics := &Metrics{defaultChannelBufferSizeMetrics}
	return defaultMetrics
}
func (m *Metrics) GetMetricsInfo(title string) string {
	report := fmt.Sprintf("%s ... Channel Buffer Size Metrics ... \n", title)
	if title == "" {
		report = fmt.Sprint("Metrics Info : Channel Buffer Size Metrics ... \n")
	}
	report += fmt.Sprintf("Monitor:[%d], Pipe:[%d], DataProcessor:[%d], DataAdapter:[%d], DataObjectStore:[%d] \n", m.ChannelBufferSize.Monitor, m.ChannelBufferSize.Pipe, m.ChannelBufferSize.DataProcessor, m.ChannelBufferSize.DataAdapter, m.ChannelBufferSize.DataObjectStore)
	return report
}
