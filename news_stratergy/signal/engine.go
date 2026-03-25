package signal

import (
	"fmt"
	"time"

	"news_strategy/common"
)

type Engine struct {
	rules common.RulesConfig
}

func New(rules common.RulesConfig) *Engine {
	return &Engine{rules: rules}
}

// Evaluate converts a scored event into an actionable watch/long/short decision.
func (e *Engine) Evaluate(event common.ClassifiedEvent, score common.EventScore) common.EventSignal {
	signalType := "IGNORE"
	reason := fmt.Sprintf("%s event with %.2f score", event.EventType, score.FinalScore)

	// Respect the core architecture rule: without live market confirmation this
	// MVP can only surface a WATCH, never a trade candidate.
	hasMarketConfirmation := score.PriceConfirmation > 0 || score.VolumeConfirmation > 0
	switch {
	case hasMarketConfirmation && event.Sentiment == "positive" && score.FinalScore >= e.rules.LongThreshold:
		signalType = "LONG_CANDIDATE"
	case hasMarketConfirmation && event.Sentiment == "negative" && score.FinalScore >= e.rules.ShortThreshold:
		signalType = "SHORT_CANDIDATE"
	case score.FinalScore >= e.rules.WatchThreshold:
		signalType = "WATCH"
		if !hasMarketConfirmation {
			reason = fmt.Sprintf("%s event with %.2f score; waiting for market confirmation", event.EventType, score.FinalScore)
		}
	}
	return common.EventSignal{
		EventID:    event.EventID,
		Symbol:     event.Symbol,
		SignalType: signalType,
		Status:     "CREATED",
		Score:      score.FinalScore,
		Reason:     reason,
		CreatedAt:  time.Now(),
	}
}
