package classifier

import (
	"strings"
	"time"

	"news_strategy/common"
)

type Engine struct{}

func New() *Engine { return &Engine{} }

// Normalize cleans the raw inbound item into a stable internal event shape.
func (e *Engine) Normalize(item common.RawNewsItem) common.NormalizedNewsItem {
	publishedAt := item.PublishedAt
	if publishedAt.IsZero() {
		publishedAt = time.Now()
	}
	return common.NormalizedNewsItem{
		EventID:     buildEventID(item),
		Source:      strings.TrimSpace(item.Source),
		SourceType:  strings.TrimSpace(item.SourceType),
		Title:       clean(item.Title),
		Summary:     clean(item.Summary),
		Body:        clean(item.Body),
		URL:         strings.TrimSpace(item.URL),
		PublishedAt: publishedAt,
		CompanyName: clean(item.CompanyName),
		Symbol:      strings.ToUpper(strings.TrimSpace(item.Symbol)),
	}
}

// Classify applies rule-based event typing, sentiment, severity, and confidence.
func (e *Engine) Classify(item common.NormalizedNewsItem) common.ClassifiedEvent {
	text := strings.ToLower(strings.Join([]string{item.Title, item.Summary, item.Body}, " "))
	eventType := "GENERAL_NEWS"
	sentiment := "neutral"
	severity := 2
	confidence := 0.55
	metadata := map[string]string{}

	switch {
	case containsAny(text, "special dividend", "interim dividend", "final dividend", "dividend declared"):
		eventType = "DIVIDEND_DECLARED"
		sentiment = "positive"
		severity = 4
		confidence = 0.90
	case containsAny(text, "record date", "ex-date", "ex date"):
		eventType = "DIVIDEND_RECORD_DATE"
		sentiment = "neutral"
		severity = 3
		confidence = 0.82
	case containsAny(text, "quarterly results", "financial results", "q1", "q2", "q3", "q4", "ebitda", "pat"):
		eventType = "RESULT_DECLARED"
		severity = 4
		confidence = 0.84
		if containsAny(text, "beat", "strong", "margin expansion", "guidance up") {
			sentiment = "positive"
			confidence = 0.88
		}
		if containsAny(text, "miss", "weak", "guidance cut", "margin compression") {
			sentiment = "negative"
			confidence = 0.88
		}
	case containsAny(text, "fire", "raid", "fraud", "cyberattack", "resigns", "steps down", "downgrade", "penalty", "probe"):
		eventType = "NEGATIVE_INCIDENT"
		sentiment = "negative"
		severity = 5
		confidence = 0.92
	case containsAny(text, "acquires", "acquisition", "merger", "upgrade", "stake buy", "wins order"):
		eventType = "POSITIVE_INCIDENT"
		sentiment = "positive"
		severity = 4
		confidence = 0.86
	}

	metadata["classification_version"] = "rules_v1"
	return common.ClassifiedEvent{
		EventID:     item.EventID,
		Symbol:      item.Symbol,
		CompanyName: item.CompanyName,
		EventType:   eventType,
		Sentiment:   sentiment,
		Severity:    severity,
		Confidence:  confidence,
		Metadata:    metadata,
		Title:       item.Title,
		Summary:     item.Summary,
		Source:      item.Source,
		SourceType:  item.SourceType,
		PublishedAt: item.PublishedAt,
		URL:         item.URL,
	}
}

func clean(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func containsAny(text string, values ...string) bool {
	for _, value := range values {
		if strings.Contains(text, value) {
			return true
		}
	}
	return false
}

func buildEventID(item common.RawNewsItem) string {
	key := strings.ToLower(strings.TrimSpace(item.Source + "|" + item.Title + "|" + item.URL))
	key = strings.ReplaceAll(key, " ", "_")
	if len(key) > 48 {
		key = key[:48]
	}
	return "evt_" + key
}
