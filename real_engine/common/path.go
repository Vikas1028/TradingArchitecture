package common

import (
	"fmt"
	"os"
	"path/filepath"
)

func ResolveConfigPath() (string, error) {
	return resolvePath(ConfigDirName, ConfigFileName)
}

func ResolveLoggerConfigPath() (string, error) {
	return resolvePath(LoggerConfigDirName, LoggerConfigFileName)
}

func ResolveAppRoot() (string, error) {
	configPath, err := ResolveConfigPath()
	if err != nil {
		return "", err
	}

	configDir := filepath.Dir(configPath)
	if filepath.Base(configDir) == ConfigDirName {
		return filepath.Dir(configDir), nil
	}
	return configDir, nil
}

func resolvePath(dirName, fileName string) (string, error) {
	workingDir, _ := os.Getwd()
	executablePath, _ := os.Executable()
	executableDir := filepath.Dir(executablePath)

	candidates := []string{
		filepath.Join(workingDir, dirName, fileName),
		filepath.Join(workingDir, fileName),
		filepath.Join(executableDir, fileName),
		filepath.Join(executableDir, dirName, fileName),
		filepath.Join(executableDir, "..", dirName, fileName),
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("unable to locate %s", fileName)
}
