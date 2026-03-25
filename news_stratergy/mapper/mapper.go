package mapper

import (
	"strings"

	"news_strategy/common"
)

type Engine struct {
	aliasToSymbol map[string]string
}

// New builds the alias lookup used to map company text to tradable symbols.
func New(cfg common.SymbolsConfig) *Engine {
	aliasToSymbol := make(map[string]string)
	for symbol, aliases := range cfg.Aliases {
		normalizedSymbol := strings.ToUpper(strings.TrimSpace(symbol))
		aliasToSymbol[strings.ToLower(normalizedSymbol)] = normalizedSymbol
		for _, alias := range aliases {
			aliasToSymbol[strings.ToLower(strings.TrimSpace(alias))] = normalizedSymbol
		}
	}
	return &Engine{aliasToSymbol: aliasToSymbol}
}

// Map resolves the best-effort stock symbol for the classified event.
func (e *Engine) Map(event common.ClassifiedEvent) common.ClassifiedEvent {
	if strings.TrimSpace(event.Symbol) != "" {
		event.Symbol = strings.ToUpper(strings.TrimSpace(event.Symbol))
		return event
	}
	candidates := []string{
		event.CompanyName,
		event.Title,
		event.Summary,
	}
	for _, candidate := range candidates {
		lower := strings.ToLower(candidate)
		for alias, symbol := range e.aliasToSymbol {
			if strings.Contains(lower, alias) {
				event.Symbol = symbol
				return event
			}
		}
	}
	return event
}
