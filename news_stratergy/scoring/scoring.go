package scoring

import (
	"strings"
	"time"

	"news_strategy/common"
)

type Engine struct{}

func New() *Engine { return &Engine{} }

// Score turns the classified event and current market reaction into a weighted score.
func (e *Engine) Score(event common.ClassifiedEvent, reaction *common.MarketReaction) common.EventScore {
	sourceReliability := 60.0
	switch strings.ToLower(event.SourceType) {
	case "official_exchange", "official":
		sourceReliability = 95
	case "news_api", "financial_news":
		sourceReliability = 72
	}

	severityScore := (float64(event.Severity) / 5.0) * 100.0
	confidenceScore := event.Confidence * 100.0
	freshnessMinutes := time.Since(event.PublishedAt).Minutes()
	freshnessScore := 100.0
	if freshnessMinutes > 0 {
		freshnessScore = maxFloat(0, 100-(freshnessMinutes*2))
	}

	sentimentStrength := 0.0
	switch event.Sentiment {
	case "positive", "negative":
		sentimentStrength = 80
	}

	priceConfirmation := 0.0
	volumeConfirmation := 0.0
	if reaction != nil {
		priceConfirmation = reaction.PriceConfirmation
		volumeConfirmation = reaction.VolumeConfirmation
	}

	final := sourceReliability*0.25 +
		severityScore*0.20 +
		confidenceScore*0.20 +
		freshnessScore*0.20 +
		sentimentStrength*0.15 +
		priceConfirmation*0.10 +
		volumeConfirmation*0.10

	return common.EventScore{
		EventID:            event.EventID,
		Symbol:             event.Symbol,
		FinalScore:         final,
		SourceReliability:  sourceReliability,
		SeverityScore:      severityScore,
		ConfidenceScore:    confidenceScore,
		FreshnessScore:     freshnessScore,
		SentimentStrength:  sentimentStrength,
		PriceConfirmation:  priceConfirmation,
		VolumeConfirmation: volumeConfirmation,
		ComputedAt:         time.Now(),
	}
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
