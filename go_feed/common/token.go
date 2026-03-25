package common

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadSubscriptionTokens loads Nifty basket tokens from CSV files and prepares grouped subscription lists.
func LoadSubscriptionTokens() (*SubscriptionTokens, error) {
	tokenMap, err := loadTokenMaster()
	if err != nil {
		tokenMap = make(map[string]string)
	}

	nifty50, err := loadTokenGroup(Nifty50FileName, tokenMap)
	if err != nil {
		return nil, err
	}
	nifty100, err := loadTokenGroup(Nifty100FileName, tokenMap)
	if err != nil {
		return nil, err
	}
	nifty200, err := loadTokenGroup(Nifty200FileName, tokenMap)
	if err != nil {
		return nil, err
	}
	nifty300, err := loadTokenGroup(Nifty300FileName, tokenMap)
	if err != nil {
		return nil, err
	}
	nifty500, err := loadTokenGroup(Nifty500FileName, tokenMap)
	if err != nil {
		return nil, err
	}

	return &SubscriptionTokens{
		Nifty50:  nifty50,
		Nifty100: nifty100,
		Nifty200: nifty200,
		Nifty300: nifty300,
		Nifty400: buildNifty400Tokens(nifty500),
		Nifty500: nifty500,
	}, nil
}

// GetTokensForGroup returns the cumulative token list for the requested Nifty basket size.
func GetTokensForGroup(groupName string, tokenSet *SubscriptionTokens) []TokenInfo {
	if tokenSet == nil {
		return nil
	}

	switch strings.ToLower(strings.TrimSpace(groupName)) {
	case TokenGroupNifty50:
		return cloneTokenSlice(tokenSet.Nifty50)
	case TokenGroupNifty100:
		return mergeTokenGroups(tokenSet.Nifty50, tokenSet.Nifty100)
	case TokenGroupNifty200:
		return mergeTokenGroups(tokenSet.Nifty50, tokenSet.Nifty100, tokenSet.Nifty200)
	case TokenGroupNifty300:
		return mergeTokenGroups(tokenSet.Nifty50, tokenSet.Nifty100, tokenSet.Nifty200, tokenSet.Nifty300)
	case TokenGroupNifty400:
		return mergeTokenGroups(tokenSet.Nifty50, tokenSet.Nifty100, tokenSet.Nifty200, tokenSet.Nifty300, tokenSet.Nifty400)
	case TokenGroupNifty500:
		return mergeTokenGroups(tokenSet.Nifty50, tokenSet.Nifty100, tokenSet.Nifty200, tokenSet.Nifty300, tokenSet.Nifty400, tokenSet.Nifty500)
	default:
		return nil
	}
}

// BuildTokenSymbolMap returns a reverse lookup from token/security id to symbol.
func BuildTokenSymbolMap(tokenSet *SubscriptionTokens) map[string]string {
	tokenMap := make(map[string]string)
	if tokenSet == nil {
		return tokenMap
	}

	for _, group := range [][]TokenInfo{
		tokenSet.Nifty50,
		tokenSet.Nifty100,
		tokenSet.Nifty200,
		tokenSet.Nifty300,
		tokenSet.Nifty400,
		tokenSet.Nifty500,
	} {
		for _, tokenInfo := range group {
			if tokenInfo.Token == "" || tokenInfo.Symbol == "" {
				continue
			}
			tokenMap[tokenInfo.Token] = tokenInfo.Symbol
		}
	}

	return tokenMap
}

// loadTokenMaster reads the broker token CSV and maps symbols to token values.
func loadTokenMaster() (map[string]string, error) {
	filePath, err := resolveDataFilePath(TokenDirName, TokenMasterFileName)
	if err != nil {
		filePath = ""
	}

	tokenMap := make(map[string]string)
	if filePath != "" {
		rows, err := readCSVRows(filePath)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			symbol := strings.ToUpper(strings.TrimSpace(row[CSVColumnSymbol]))
			token := strings.TrimSpace(row[CSVColumnToken])
			if symbol == "" || token == "" {
				continue
			}
			tokenMap[symbol] = token
		}
	}

	if err := augmentTokenMapFromBasket(tokenMap, Nifty500FileName); err != nil {
		return nil, err
	}

	return tokenMap, nil
}

// loadTokenGroup reads one stock basket CSV and converts matching symbols into token entries.
func loadTokenGroup(fileName string, tokenMap map[string]string) ([]TokenInfo, error) {
	filePath, err := resolveTokenGroupFilePath(fileName)
	if err != nil {
		return nil, err
	}
	rows, err := readCSVRows(filePath)
	if err != nil {
		return nil, err
	}

	tokens := make([]TokenInfo, 0, len(rows))
	for _, row := range rows {
		symbol := strings.ToUpper(strings.TrimSpace(row[CSVColumnSymbol]))
		if symbol == "" {
			continue
		}

		// Prefer token values embedded in the stock basket CSV so larger groups
		// like nifty500 can subscribe fully without relying on a small token master.
		tokenValue := strings.TrimSpace(row[CSVColumnToken])
		if tokenValue == "" {
			var found bool
			tokenValue, found = tokenMap[symbol]
			if !found {
				continue
			}
		}

		tokens = append(tokens, TokenInfo{
			Symbol: symbol,
			Token:  tokenValue,
		})
	}
	return tokens, nil
}

// buildNifty400Tokens derives a Nifty 400 basket from the first 400 symbols of the Nifty 500 list.
func buildNifty400Tokens(nifty500 []TokenInfo) []TokenInfo {
	if len(nifty500) <= Nifty400Count {
		return cloneTokenSlice(nifty500)
	}
	return cloneTokenSlice(nifty500[:Nifty400Count])
}

// mergeTokenGroups combines multiple token slices while keeping each symbol only once.
func mergeTokenGroups(groups ...[]TokenInfo) []TokenInfo {
	merged := make([]TokenInfo, 0)
	seen := make(map[string]bool)

	for _, group := range groups {
		for _, tokenInfo := range group {
			if seen[tokenInfo.Symbol] {
				continue
			}
			seen[tokenInfo.Symbol] = true
			merged = append(merged, tokenInfo)
		}
	}
	return merged
}

// cloneTokenSlice returns a copy of a token slice so callers can modify it safely.
func cloneTokenSlice(values []TokenInfo) []TokenInfo {
	cloned := make([]TokenInfo, len(values))
	copy(cloned, values)
	return cloned
}

// readCSVRows reads a CSV file and converts each row into a header-based key/value map.
func readCSVRows(filePath string) ([]map[string]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("csv file is empty: %s", filePath)
	}

	headers := records[0]
	rows := make([]map[string]string, 0, len(records)-1)
	for _, record := range records[1:] {
		row := make(map[string]string)
		for index, header := range headers {
			if index >= len(record) {
				row[header] = ""
				continue
			}
			row[header] = record[index]
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func resolveDataFilePath(folderName, fileName string) (string, error) {
	appRoot, err := ResolveAppRoot()
	if err != nil {
		return "", err
	}

	candidates := []string{
		filepath.Join(appRoot, folderName, fileName),
		filepath.Join(appRoot, "..", folderName, fileName),
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("unable to locate %s/%s", folderName, fileName)
}

func resolveTokenGroupFilePath(fileName string) (string, error) {
	appRoot, err := ResolveAppRoot()
	if err != nil {
		return "", err
	}

	candidates := []string{
		filepath.Join(appRoot, "..", "python_feed", "config", fileName),
		filepath.Join(appRoot, StocksDirName, fileName),
		filepath.Join(appRoot, "..", StocksDirName, fileName),
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("unable to locate subscription token file for %s", fileName)
}

func augmentTokenMapFromBasket(tokenMap map[string]string, fileName string) error {
	filePath, err := resolveTokenGroupFilePath(fileName)
	if err != nil {
		if len(tokenMap) == 0 {
			return err
		}
		return nil
	}

	rows, err := readCSVRows(filePath)
	if err != nil {
		return err
	}

	for _, row := range rows {
		symbol := strings.ToUpper(strings.TrimSpace(row[CSVColumnSymbol]))
		token := strings.TrimSpace(row[CSVColumnToken])
		if symbol == "" || token == "" {
			continue
		}
		tokenMap[symbol] = token
	}

	return nil
}
