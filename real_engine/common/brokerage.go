package common

// BrokerageForTrade is a placeholder extension point for live-cost modelling.
// The current engine stores broker-reported fills separately, so this returns zero
// until a charge model is intentionally added.
func BrokerageForTrade(price float64, quantity int64) float64 {
	_ = price
	_ = quantity
	return 0
}
