package main

import (
	"encoding/json"
	"fmt"
	"strconv"

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
	Bid orderStruct `json:"bid"`
	Ask orderStruct `json:"ask"`
}

type orderStruct struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

func main() {
	pairURL := fmt.Sprintf("%s/%s@depth", wsURL, btcusdtPair)

	conn, _, err := websocket.DefaultDialer.Dial(pairURL, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for {
		_, msgByte, err := conn.ReadMessage()
		if err != nil {
			log.Errorf("Received error from ws: %s\n", err)
			break
		}
		event := &depthEventStruct{}
		if err := json.Unmarshal(msgByte, event); err != nil {
			log.Errorf("Error unmarshaling event: %s\n", err)
			break
		}
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
				log.Errorf("Couldn't parse bid quantity: %s", err)
				continue
			}
			// if quantity equals to zero, no need to add order
			if bidQuantity == 0 {
				continue
			}
			// get order price
			bidPrice, err := strconv.ParseFloat(bid[0], 64)
			if err != nil {
				log.Errorf("Couldn't parse bid price: %s", err)
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
		for _, ask := range event.Asks[1:] {
			// check if order is valid
			if len(ask) < 2 {
				continue
			}
			// get order quantity
			askQuantity, err := strconv.ParseFloat(ask[1], 64)
			if err != nil {
				log.Errorf("Couldn't parse ask quantity: %s", err)
				continue
			}
			// if quantity equals to zero, no need to add order
			if askQuantity == 0 {
				continue
			}
			// get order price
			askPrice, err := strconv.ParseFloat(ask[0], 64)
			if err != nil {
				log.Errorf("Couldn't parse ask price: %s", err)
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
		pairDepth := pairDepthStruct{
			Bid: orderStruct{highestBid, highestBidQuantity},
			Ask: orderStruct{lowestAsk, lowestAskQuantity},
		}
		pairDepthByte, err := json.Marshal(pairDepth)
		if err != nil {
			log.Errorf("Couldn't marshal pair depth: %s", err)
			break
		}
		log.Infof("%s\n", string(pairDepthByte))
	}
	log.Infof("Finished")
}
