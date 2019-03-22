package metrics

import (
	"time"

	"github.com/mytokenio/go/log"
)

func Count(id string, delta int64) {
	if id == "" {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := countMap[id]; !ok {
		countMap[id] = delta
	} else {
		countMap[id] += delta
	}
}

func Gauge(id string, value interface{}) {
	if id == "" {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	switch value.(type) {
	case uint8:
		gaugeIntMap[id] = int64(value.(uint8))
	case uint16:
		gaugeIntMap[id] = int64(value.(uint16))
	case uint32:
		gaugeIntMap[id] = int64(value.(uint32))
	case uint64:
		gaugeIntMap[id] = int64(value.(uint64))
	case uint:
		gaugeIntMap[id] = int64(value.(uint))
	case int8:
		gaugeIntMap[id] = int64(value.(int8))
	case int16:
		gaugeIntMap[id] = int64(value.(int16))
	case int32:
		gaugeIntMap[id] = int64(value.(int32))
	case int64:
		gaugeIntMap[id] = int64(value.(int64))
	case int:
		gaugeIntMap[id] = int64(value.(int))
	case string:
		gaugeStrMap[id] = value.(string)
	default:
		return
	}
}

func Close() {
	log.Debug("metrics Close()")

	Gauge("stop_time", time.Now().Unix())

	go func() {

		// report cache data
		reportStateFactory()

		// wait for send msg
		time.Sleep(500 * time.Millisecond)

		// resource recovery
		if exitChan != nil {
			close(exitChan)
		}
		if globalKafka.chanStateProducerValue != nil {
			close(globalKafka.chanStateProducerValue)
		}
		if globalKafka.chanAlarmProducerValue != nil {
			close(globalKafka.chanAlarmProducerValue)
		}
		if globalKafka.producer != nil {
			globalKafka.producer.Close()
		}

		closeGlobalMap()
	}()

	time.Sleep(1000 * time.Millisecond)
}

// ---------------------------------------------------------------------------------------------------------------------

func GetCount(id string) (int64, bool) {
	mutex.Lock()
	defer mutex.Unlock()

	if v, ok := countMap[id]; !ok {
		return 0, false
	} else {
		return v, true
	}
}

func GetGaugeStr(id string) (string, bool) {
	mutex.Lock()
	defer mutex.Unlock()

	if v, ok := gaugeStrMap[id]; !ok {
		return "", false
	} else {
		return v, true
	}
}

// ---------------------------------------------------------------------------------------------------------------------

func StatusOK() {
	log.Debug("metrics StatusOK()")

	Gauge("status", STATUS_OK)
}

func StatusError() {
	log.Debug("metrics StatusError()")

	Gauge("status", STATUS_ERROR)
}

func ExitWithOK() {
	log.Debug("metrics ExitWithOK()")

	Gauge("status", STATUS_OK)
	Gauge("exit_code", EXIT_CODE_OK)
}

func ExitWithErr(alarmMsg ...string) {
	log.Debug("metrics ExitWithErr()")

	if len(alarmMsg) > 0 {
		alarm(alarmMsg[0])
	}
	Gauge("status", STATUS_ERROR)
	Gauge("exit_code", EXIT_CODE_ERROR)
}

func ExitWithKill(alarmMsg ...string) {
	log.Debug("metrics ExitWithKill()")

	if len(alarmMsg) > 0 {
		alarm(alarmMsg[0])
	}
	Gauge("status", STATUS_ERROR)
	Gauge("exit_code", EXIT_CODE_KILL)
}

func Panic(err error) {
	if err != nil {
		log.Debugf("metrics Panic(%s)", err.Error())

		alarm(err.Error())
		Gauge("status", STATUS_ERROR)
		Gauge("exit_code", EXIT_CODE_ERROR)
		panic(err)
	}
}

func Alarm(alarmMsg string) {
	if len(alarmMsg) > 0 {
		log.Debugf("metrics Alarm(%s)", alarmMsg)

		alarm(alarmMsg)
	}
}

// ---------------------------------------------------------------------------------------------------------------------

func closeGlobalMap() {
	mutex.Lock()
	defer mutex.Unlock()

	for key, _ := range countMap {
		delete(countMap, key)
	}

	for key, _ := range gaugeIntMap {
		delete(gaugeIntMap, key)
	}

	for key, _ := range gaugeStrMap {
		delete(gaugeStrMap, key)
	}
}
