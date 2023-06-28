package logger

import (
	"fmt"
	"os"
	"time"
)

func CreateLogsDirectory(logPath string) (string, error) {
	// Get current user information
	/*
		usr, err := user.Current()
		if err != nil {
			return "", err
		}
	*/
	// Create logs directory path
	logsDir := logPath

	// Create the logs directory
	err := os.MkdirAll(logsDir, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		return "", err
	}
	return logsDir, nil
}

func CreateLogFile(logsDir string, fileName string) (*os.File, error) {
	// Create log file name based on the current date
	currentTime := time.Now()
	logFileName := fileName + "." + currentTime.Format("2006-01-02") + ".log"

	// Create the log file path
	logFile, err := os.OpenFile(logsDir+"/"+logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	fmt.Println("Created log file:", logFileName)
	return logFile, nil
}
