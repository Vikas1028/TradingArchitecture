package common

import (
	"fmt"
	"os"
	"path/filepath"
)

func ResolveConfigPath() (string, error) {
	return resolveNamedConfigPath(ConfigFileName)
}

func ResolveLoggerConfigPath() (string, error) {
	return resolveNamedConfigPath(LoggerConfigFileName)
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
