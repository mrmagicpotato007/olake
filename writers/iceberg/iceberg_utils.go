package iceberg

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/writers/iceberg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// portMap tracks which ports are in use
var portMap sync.Map

func findAvailablePort() (int, error) {
	for p := 50051; p <= 59051; p++ {
		// Try to store port in map - returns false if already exists
		if _, loaded := portMap.LoadOrStore(p, true); !loaded {
			// Check if the port is already in use by another process
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", p), time.Second)
			if err == nil {
				// Port is in use, close our test connection
				conn.Close()

				// Find the process using this port
				cmd := exec.Command("lsof", "-i", fmt.Sprintf(":%d", p), "-t")
				output, err := cmd.Output()
				if err != nil {
					// Failed to find process, continue to next port
					portMap.Delete(p)
					continue
				}

				// Get the PID
				pid := strings.TrimSpace(string(output))
				if pid == "" {
					// No process found, continue to next port
					portMap.Delete(p)
					continue
				}

				// Kill the process
				killCmd := exec.Command("kill", "-9", pid)
				err = killCmd.Run()
				if err != nil {
					logger.Warn("Failed to kill process using port %d: %v", p, err)
					portMap.Delete(p)
					continue
				}

				logger.Info("Killed process %s that was using port %d", pid, p)

				// Wait a moment for the port to be released
				time.Sleep(time.Second * 5)
			}
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available ports found between 50051 and 59051")
}

func (i *Iceberg) SetupIcebergClient(upsert bool) error {
	// Create JSON config for the Java server
	err := i.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	// Find an available port
	port, err := findAvailablePort()
	if err != nil {
		return err
	}

	i.port = port

	// Add port to server config
	serverConfig := map[string]string{
		"port":                 fmt.Sprintf("%d", port),
		"catalog-impl":         "org.apache.iceberg.aws.glue.GlueCatalog",
		"io-impl":              "org.apache.iceberg.aws.s3.S3FileIO",
		"warehouse":            i.config.S3Path,
		"table-namespace":      i.config.Database,
		"catalog-name":         "olake_iceberg",
		"glue.region":          i.config.Region,
		"s3.access-key-id":     i.config.AccessKey,
		"s3.secret-access-key": i.config.SecretKey,
		"table-prefix":         "",
		"upsert":               strconv.FormatBool(upsert),
		"upsert-keep-deletes":  "true",
		"write.format.default": "parquet",
		"s3.path-style-access": "true",
	}

	if i.config.SessionToken != "" {
		serverConfig["aws.session-token"] = i.config.SessionToken
	}

	configJSON, err := json.Marshal(serverConfig)
	if err != nil {
		return fmt.Errorf("failed to create server config: %v", err)
	}

	// Start the Java server process
	i.cmd = exec.Command("java", "-jar", i.config.JarPath, string(configJSON))
	i.cmd.Stdout = os.Stdout
	i.cmd.Stderr = os.Stderr

	if err := i.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start Iceberg server: %v", err)
	}

	// Connect to gRPC server
	conn, err := grpc.NewClient(`localhost:`+strconv.Itoa(i.port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	if err != nil {
		logger.Errorf("failed to connect to iceberg writer: %v", err)
	}

	logger.Info("Connected to iceberg writer", "port", i.port)
	i.conn = conn
	i.client = proto.NewStringArrayServiceClient(conn)
	return nil
}

func getTestDebeziumRecord() string {
	random_id := utils.ULID()
	return `{
			"destination_table": "olake_test_table",
			"key": {
				"schema" : {
						"type" : "struct",
						"fields" : [ {
							"type" : "string",
							"optional" : true,
							"field" : "_id"
						} ],
						"optional" : false
					},
					"payload" : {
						"_id" : "` + random_id + `"
					}
				}
				,
			"value": {
				"schema" : {
					"type" : "struct",
					"fields" : [ {
					"type" : "string",
					"optional" : true,
					"field" : "_id"
					}, {
					"type" : "boolean",
					"optional" : true,
					"field" : "__deleted"
					}, {
					"type" : "string",
					"optional" : true,
					"field" : "__op"
					}, {
					"type" : "string",
					"optional" : true,
					"field" : "__db"
					}, {
					"type" : "int64",
					"optional" : true,
					"field" : "__source_ts_ms"
					} ],
					"optional" : false,
					"name" : "dbz_.incr.incr1"
				},
				"payload" : {
					"_id" : "` + random_id + `",
					"__deleted" : false,
					"__op" : "r",
					"__db" : "incr",
					"__source_ts_ms" : 1738502494009
				}
			}
		}`
}

func (i *Iceberg) CloseIcebergClient() error {
	if i.conn != nil {
		i.conn.Close()
	}

	if i.cmd != nil && i.cmd.Process != nil {
		err := i.cmd.Process.Kill()
		if err != nil {
			return fmt.Errorf("failed to kill iceberg server: %v", err)
		}
	}

	return nil
}
