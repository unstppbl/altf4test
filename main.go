package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gorilla/websocket"
)

const (
	btcusdtPair = "btcusdt"
	ethusdtPair = "ethusdt"
	xlmusdtPair = "xlmusdt"
	wsURL       = "wss://stream.binance.com:9443/ws"
)

type depthEventStruct struct {
	Type          string     `json:"e"`
	TimeUnix      int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

type pairDepthStruct struct {
	pair string
	time string
	Bid  orderStruct `json:"bid"`
	Ask  orderStruct `json:"ask"`
}

type orderStruct struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

func main() {
	// make a slice of pairs (can be replaced with taking pairs from config file)
	pairs := []string{btcusdtPair, ethusdtPair, xlmusdtPair}

	getDepthData(pairs)

	log.Infof("Finished")
}

func getDepthData(pairs []string) {
	// create channels slice and start getting messages
	var channels []chan *pairDepthStruct
	for _, pair := range pairs {
		pairCh := make(chan *pairDepthStruct)
		go startPairStream(pair, pairCh)
		channels = append(channels, pairCh)
	}
	// prepare select cases to iterate over
	selectCases := make([]reflect.SelectCase, len(channels))
	for i, ch := range channels {
		selectCases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	// get messages
	remaining := len(selectCases)
	for remaining > 0 {
		chosen, value, ok := reflect.Select(selectCases)
		if !ok {
			// the channel closed, remove it to disable the case
			selectCases[chosen].Chan = reflect.ValueOf(nil)
			remaining--
			continue
		}
		pairDepth, ok := value.Interface().(*pairDepthStruct)
		if !ok {
			log.Error("Couldn't convert value received from channel\n")
			continue
		}
		pairDepthByte, err := json.Marshal(pairDepth)
		if err != nil {
			log.Errorf("Couldn't marshal pair depth: %s", err)
			break
		}
		log.Infof("%s - %s\n", pairDepth.pair, pairDepth.time)
		log.Infof("%s\n\n", string(pairDepthByte))
	}
}

func startPairStream(pair string, pairCh chan<- *pairDepthStruct) {
	log.Infof("Starting %s depth stream\n\n", pair)
	// close channel before returning from function
	defer close(pairCh)
	// form pair websocket url
	pairURL := fmt.Sprintf("%s/%s@depth", wsURL, pair)
	// get connection to ws
	conn, _, err := websocket.DefaultDialer.Dial(pairURL, nil)
	if err != nil {
		log.Errorf("Couldn't get ws connection: %s", err)
		return
	}
	// close connection before returning from function
	defer conn.Close()

	// receive messages from ws
	for {
		_, msgByte, err := conn.ReadMessage()
		if err != nil {
			log.Errorf("Error receiving msg from ws: %s", err)
			return
		}
		event := &depthEventStruct{}
		if err := json.Unmarshal(msgByte, event); err != nil {
			log.Errorf("Error unmarshaling event: %s\n", err)
			continue
		}
		pairDepth := getPairDepthFromEvent(event)
		// send event to channel
		pairCh <- pairDepth
	}
}

func getPairDepthFromEvent(event *depthEventStruct) (pairDepth *pairDepthStruct) {
	// get highest bid price and amount
	var highestBid float64
	var highestBidQuantity float64
	for _, bid := range event.Bids {
		// check if order is valid
		if len(bid) < 2 {
			continue
		}
		// get order quantity
		bidQuantity, err := strconv.ParseFloat(bid[1], 64)
		if err != nil {
			log.Errorf("Couldn't parse bid quantity: %s\n", err)
			continue
		}
		// if quantity equals to zero, no need to add order
		if bidQuantity == 0 {
			continue
		}
		// get order price
		bidPrice, err := strconv.ParseFloat(bid[0], 64)
		if err != nil {
			log.Errorf("Couldn't parse bid price: %s\n", err)
			continue
		}
		// update highest price and quantity
		if bidPrice > highestBid {
			highestBid = bidPrice
			highestBidQuantity = bidQuantity
		}
	}
	// get lowest ask price and amount
	var lowestAsk float64
	var lowestAskQuantity float64
	for _, ask := range event.Asks {
		// check if order is valid
		if len(ask) < 2 {
			continue
		}
		// get order quantity
		askQuantity, err := strconv.ParseFloat(ask[1], 64)
		if err != nil {
			log.Errorf("Couldn't parse ask quantity: %s\n", err)
			continue
		}
		// if quantity equals to zero, no need to add order
		if askQuantity == 0 {
			continue
		}
		// get order price
		askPrice, err := strconv.ParseFloat(ask[0], 64)
		if err != nil {
			log.Errorf("Couldn't parse ask price: %s\n", err)
			continue
		}
		// update lowest price and quantity
		if lowestAsk == 0 {
			lowestAsk = askPrice
			lowestAskQuantity = askQuantity
			continue
		}
		if askPrice < lowestAsk {
			lowestAsk = askPrice
			lowestAskQuantity = askQuantity
		}
	}
	pairDepth = &pairDepthStruct{
		Bid:  orderStruct{highestBid, highestBidQuantity},
		Ask:  orderStruct{lowestAsk, lowestAskQuantity},
		pair: event.Symbol,
		time: time.Now().Format(time.StampMilli),
	}
	return pairDepth
}
