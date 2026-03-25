package feed

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"go_ltp/common"
)

// ParseTick converts a raw Dhan websocket packet into a normalized tick structure.
func ParseTick(payload []byte, symbolByToken map[string]string) (*common.DhanTick, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf("payload too short: %d", len(payload))
	}

	responseCode := payload[0]
	switch responseCode {
	case common.DhanFeedCodeQuote, common.DhanFeedCodeFull:
	default:
		return nil, fmt.Errorf("unsupported response code: %d", responseCode)
	}

	tick := &common.DhanTick{
		ResponseCode:    responseCode,
		MessageLength:   int16(binary.LittleEndian.Uint16(payload[1:3])),
		ExchangeSegment: payload[3],
		SecurityID:      fmt.Sprintf("%d", binary.LittleEndian.Uint32(payload[4:8])),
		FeedTimestamp:   time.Now(),
		RawPayload:      append([]byte(nil), payload...),
	}
	tick.Symbol = symbolByToken[tick.SecurityID]

	if len(payload) >= 12 {
		tick.LastTradedPrice = parseFloat32(payload[8:12])
	}
	if len(payload) >= 14 {
		tick.LastTradeQuantity = int16(binary.LittleEndian.Uint16(payload[12:14]))
	}
	if len(payload) >= 18 {
		tick.LastTradeTime = int32(binary.LittleEndian.Uint32(payload[14:18]))
	}
	if len(payload) >= 22 {
		tick.AverageTradePrice = parseFloat32(payload[18:22])
	}
	if len(payload) >= 26 {
		tick.Volume = int32(binary.LittleEndian.Uint32(payload[22:26]))
	}
	if len(payload) >= 30 {
		tick.TotalSellQuantity = int32(binary.LittleEndian.Uint32(payload[26:30]))
	}
	if len(payload) >= 34 {
		tick.TotalBuyQuantity = int32(binary.LittleEndian.Uint32(payload[30:34]))
	}
	if len(payload) >= 38 {
		tick.OpenInterest = int32(binary.LittleEndian.Uint32(payload[34:38]))
	}
	if len(payload) >= 42 {
		tick.HighestOI = int32(binary.LittleEndian.Uint32(payload[38:42]))
	}
	if len(payload) >= 46 {
		tick.LowestOI = int32(binary.LittleEndian.Uint32(payload[42:46]))
	}
	if len(payload) >= 50 {
		tick.DayOpen = parseFloat32(payload[46:50])
	}
	if len(payload) >= 54 {
		tick.DayClose = parseFloat32(payload[50:54])
	}
	if len(payload) >= 58 {
		tick.DayHigh = parseFloat32(payload[54:58])
	}
	if len(payload) >= 62 {
		tick.DayLow = parseFloat32(payload[58:62])
	}
	if responseCode == common.DhanFeedCodeFull && len(payload) >= 162 {
		tick.Depth = parseMarketDepth(payload[62:162])
	}

	return tick, nil
}

// ToKafkaMessage converts the full tick into the smaller Kafka payload.
func ToKafkaMessage(tick *common.DhanTick) common.KafkaTickMessage {
	return common.KafkaTickMessage{
		SecurityID:        tick.SecurityID,
		Symbol:            tick.Symbol,
		ExchangeSegment:   tick.ExchangeSegment,
		Timestamp:         tick.FeedTimestamp,
		LastTradedPrice:   tick.LastTradedPrice,
		Volume:            tick.Volume,
		TotalSellQuantity: tick.TotalSellQuantity,
		TotalBuyQuantity:  tick.TotalBuyQuantity,
	}
}

func parseMarketDepth(payload []byte) []common.MarketDepthLevel {
	const levelSize = 20
	levels := make([]common.MarketDepthLevel, 0, len(payload)/levelSize)
	for offset := 0; offset+levelSize <= len(payload); offset += levelSize {
		levels = append(levels, common.MarketDepthLevel{
			BidQuantity: int32(binary.LittleEndian.Uint32(payload[offset : offset+4])),
			AskQuantity: int32(binary.LittleEndian.Uint32(payload[offset+4 : offset+8])),
			BidOrders:   int16(binary.LittleEndian.Uint16(payload[offset+8 : offset+10])),
			AskOrders:   int16(binary.LittleEndian.Uint16(payload[offset+10 : offset+12])),
			BidPrice:    parseFloat32(payload[offset+12 : offset+16]),
			AskPrice:    parseFloat32(payload[offset+16 : offset+20]),
		})
	}
	return levels
}

func parseFloat32(payload []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(payload))
}
