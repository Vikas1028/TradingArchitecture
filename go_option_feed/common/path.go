package common

import (
	"fmt"
	"os"
	"path/filepath"
)

// ResolveConfigPath finds config.cfg from common source and release execution locations.
func ResolveConfigPath() (string, error) {
	return resolveNamedConfigPath(ConfigFileName)
}

// ResolveLoggerConfigPath finds logger.json from common source and release execution locations.
func ResolveLoggerConfigPath() (string, error) {
	return resolveNamedConfigPath(LoggerConfigFileName)
}

// ResolveAppRoot returns the application root directory based on the resolved config location.
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

func resolveNamedConfigPath(fileName string) (string, error) {
	workingDir, _ := os.Getwd()
	executablePath, _ := os.Executable()
	executableDir := filepath.Dir(executablePath)

	candidates := []string{
		filepath.Join(workingDir, ConfigDirName, fileName),
		filepath.Join(workingDir, fileName),
		filepath.Join(executableDir, fileName),
		filepath.Join(executableDir, ConfigDirName, fileName),
		filepath.Join(executableDir, "..", ConfigDirName, fileName),
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("unable to locate %s", fileName)
}
