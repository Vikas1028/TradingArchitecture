package dhan

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func loadSymbolSecurityMap(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("instrument csv empty")
	}

	header := make(map[string]int)
	for i, col := range rows[0] {
		header[strings.ToLower(strings.TrimSpace(col))] = i
	}
	symbolIdx, ok1 := header["symbol"]
	tokenIdx, ok2 := header["token"]
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("instrument csv missing required columns symbol/token: %s", path)
	}

	out := make(map[string]string, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) <= tokenIdx || len(row) <= symbolIdx {
			continue
		}
		symbol := strings.ToUpper(strings.TrimSpace(row[symbolIdx]))
		token := strings.TrimSpace(row[tokenIdx])
		if symbol != "" && token != "" {
			out[symbol] = token
		}
	}
	return out, nil
}
