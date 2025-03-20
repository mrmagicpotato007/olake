package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logger zerolog.Logger

// Info writes record into os.stdout with log level INFO
func Info(v ...interface{}) {
	if len(v) == 1 {
		logger.Info().Interface("message", v[0]).Send()
	} else {
		logger.Info().Msgf("%s", v...)
	}
}

// Info writes record into os.stdout with log level INFO
func Infof(format string, v ...interface{}) {
	logger.Info().Msgf(format, v...)
}

// Debug writes record into os.stdout with log level DEBUG
func Debug(v ...interface{}) {
	logger.Debug().Msgf("%s", v...)
}

// Debugf writes record into os.stdout with log level DEBUG
func Debugf(format string, v ...interface{}) {
	logger.Debug().Msgf(format, v...)
}

// Error writes record into os.stdout with log level ERROR
func Error(v ...interface{}) {
	logger.Error().Msgf("%s", v...)
}

// Fatal writes record into os.stdout with log level ERROR and exits
func Fatal(v ...interface{}) {
	logger.Fatal().Msgf("%s", v...)
	os.Exit(1)
}

// Fatal writes record into os.stdout with log level ERROR
func Fatalf(format string, v ...interface{}) {
	logger.Fatal().Msgf(format, v...)
	os.Exit(1)
}

// Error writes record into os.stdout with log level ERROR
func Errorf(format string, v ...interface{}) {
	logger.Error().Msgf(format, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warn(v ...interface{}) {
	logger.Warn().Msgf("%s", v...)
}

// Warn writes record into os.stdout with log level WARN
func Warnf(format string, v ...interface{}) {
	logger.Warn().Msgf(format, v...)
}

func LogResponse(response *http.Response) {
	respDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		Fatal(err)
	}

	fmt.Println(string(respDump))
}

func LogRequest(req *http.Request) {
	requestDump, err := httputil.DumpRequest(req, true)
	if err != nil {
		Fatal(err)
	}

	fmt.Println(string(requestDump))
}

// CreateFile creates a new file or overwrites an existing one with the specified filename, path, extension,
func FileLogger(content any, fileName, fileExtension string) error {
	// get config folder
	filePath := viper.GetString("CONFIG_FOLDER")
	if filePath == "" {
		return fmt.Errorf("config folder is not set")
	}
	// Construct the full file path
	contentBytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %s", err)
	}

	fullPath := filepath.Join(filePath, fileName+fileExtension)

	// Create or truncate the file
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create or open file: %s", err)
	}
	defer file.Close()

	// Write data to the file
	_, err = file.Write(contentBytes)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %s", err)
	}

	return nil
}

func StatsLogger(ctx context.Context, statsFunc func() (int64, int64, int64)) {
	startTime := time.Now()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				Info("Monitoring stopped")
				return
			case <-ticker.C:
				syncedRecords, runningThreads, recordsToSync := statsFunc()
				memStats := new(runtime.MemStats)
				runtime.ReadMemStats(memStats)
				speed := float64(syncedRecords) / time.Since(startTime).Seconds()
				timeElapsed := time.Since(startTime).Seconds()
				remainingRecords := recordsToSync - syncedRecords
				estimatedSeconds := "Not Determined"
				if speed > 0 && remainingRecords >= 0 {
					estimatedSeconds = fmt.Sprintf("%.2f s", float64(remainingRecords)/speed)
				}
				stats := map[string]interface{}{
					"Running Threads":          runningThreads,
					"Synced Records":           syncedRecords,
					"Memory":                   fmt.Sprintf("%d mb", memStats.HeapInuse/(1024*1024)),
					"Speed":                    fmt.Sprintf("%.2f rps", speed),
					"Seconds Elapsed":          fmt.Sprintf("%.2f", timeElapsed),
					"Estimated Remaining Time": estimatedSeconds,
				}
				if err := FileLogger(stats, "stats", ".json"); err != nil {
					Fatalf("failed to write stats in file: %s", err)
				}
			}
		}
	}()
}

func Init() {
	// Set up timestamp for log file names
	currentTimestamp := time.Now().UTC()
	timestamp := fmt.Sprintf("%d-%02d-%02d_%02d-%02d-%02d",
		currentTimestamp.Year(), currentTimestamp.Month(), currentTimestamp.Day(),
		currentTimestamp.Hour(), currentTimestamp.Minute(), currentTimestamp.Second())

	// Configure rotating file logs
	rotatingFile := &lumberjack.Logger{
		Filename:   fmt.Sprintf("%s/logs/sync_%s/olake.log", viper.GetString("CONFIG_FOLDER"), timestamp),
		MaxSize:    100, // Max size in MB
		MaxBackups: 5,   // Number of old log files to retain
		MaxAge:     30,  // Days to retain old log files
		Compress:   true,
	}

	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }

	// Log level colors
	logColors := map[string]string{
		"debug": "\033[36m", // Cyan
		"info":  "\033[32m", // Green
		"warn":  "\033[33m", // Yellow
		"error": "\033[31m", // Red
		"fatal": "\033[31m", // Red
	}

	// Thread-safe ConsoleWriter (each goroutine gets its own copy)
	newConsoleWriter := func() zerolog.ConsoleWriter {
		return zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
			FormatLevel: func(i interface{}) string {
				level := strings.ToLower(fmt.Sprintf("%s", i))
				if color, exists := logColors[level]; exists {
					return fmt.Sprintf("%s%s\033[0m", color, strings.ToUpper(level))
				}
				return strings.ToUpper(level)
			},
			FormatMessage: func(i interface{}) string {
				switch v := i.(type) {
				case string:
					return v
				default:
					jsonMsg, err := json.Marshal(v)
					if err != nil {
						return fmt.Sprintf("failed to marshal log message: %s", err)
					}
					return string(jsonMsg)
				}
			},
			FormatTimestamp: func(i interface{}) string {
				return fmt.Sprintf("\033[90m%s\033[0m", i)
			},
		}
	}

	// MultiWriter (rotating file + console writer per goroutine)
	multiWriter := zerolog.MultiLevelWriter(rotatingFile, newConsoleWriter())

	// Create global logger (immutable after init)
	logger = zerolog.New(multiWriter).With().Timestamp().Logger()
}
