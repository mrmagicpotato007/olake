package protocol

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/piyushsingariya/relec"
	"github.com/spf13/cobra"
)

// syncCmd represents the read command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Olake sync command",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if config_ == "" {
			return fmt.Errorf("--config not passed")
		} else if destinationConfig_ == "" {
			return fmt.Errorf("--destination not passed")
		} else if catalog_ == "" {
			return fmt.Errorf("--catalog not passed")
		}

		// unmarshal source config
		if err := utils.UnmarshalFile(config_, connector.GetConfigRef()); err != nil {
			return err
		}

		// unmarshal destination config
		destinationConfig = &types.WriterConfig{}
		if err := utils.UnmarshalFile(destinationConfig_, destinationConfig); err != nil {
			return err
		}

		catalog = &types.Catalog{}
		if err := utils.UnmarshalFile(catalog_, catalog); err != nil {
			return err
		}

		// default state
		state = &types.State{
			Type: types.StreamType,
		}
		if state_ != "" {
			if err := utils.UnmarshalFile(state_, state); err != nil {
				return err
			}
		}

		// TODO: state formatting
		logger.Infof("Running sync with state: %v", state)

		state.Mutex = &sync.Mutex{}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		pool, err := NewWriter(cmd.Context(), destinationConfig)
		if err != nil {
			return err
		}
		//start monitoring for stats in go routine
		go func() {

			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-cmd.Context().Done():
					fmt.Println("Monitoring stopped")
					return
				case <-ticker.C:
					file, err := os.OpenFile("/Users/datazip/Desktop/olake-1/drivers/mongodb/examples/stats.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
					if err != nil {
						fmt.Printf("failed to open stats file:%v\n", err)
					}
					defer file.Close()
					syncedRecords := pool.recordCount.Load()
					runningThreads := pool.threadCounter.Load()
					memStats := new(runtime.MemStats)
					runtime.ReadMemStats(memStats)

					stats := fmt.Sprintf(
						"Synced Records: %d\nRunning Threads: %d\nMemory Alloc: %d KB\nTotal Memory Alloc: %d KB\nHeap In Use: %d KB\n\n",
						syncedRecords, runningThreads, memStats.Alloc/1024, memStats.TotalAlloc/1024, memStats.HeapInuse/1024,
					)

					if _, err := file.WriteString(stats); err != nil {
						fmt.Printf("Failed to write to stats file: %v\n", err)
						return
					}

				}
			}

		}()

		// setup conector first
		err = connector.Setup()
		if err != nil {
			return err
		}
		// Get Source Streams
		streams, err := connector.Discover(false)
		if err != nil {
			return err
		}

		streamsMap := types.StreamsToMap(streams...)

		// Validating Streams and attaching State
		selectedStreams := []string{}
		cdcStreams := []Stream{}
		standardModeStreams := []Stream{}
		_, _ = utils.ArrayContains(catalog.Streams, func(elem *types.ConfiguredStream) bool {
			source, found := streamsMap[elem.ID()]
			if !found {
				logger.Warnf("Skipping; Configured Stream %s not found in source", elem.ID())
				return false
			}

			err := elem.Validate(source)
			if err != nil {
				logger.Warnf("Skipping; Configured Stream %s found invalid due to reason: %s", elem.ID(), err)
				return false
			}

			elem.SetupState(state)
			selectedStreams = append(selectedStreams, elem.ID())
			if elem.SyncMode == types.CDC {
				cdcStreams = append(cdcStreams, elem)
			} else {
				standardModeStreams = append(standardModeStreams, elem)
			}

			return false
		})

		logger.Infof("Valid selected streams are %s", strings.Join(selectedStreams, ", "))

		// Execute driver ChangeStreams mode
		GlobalCxGroup.Add(func(_ context.Context) error { // context is not used to keep processes mutually exclusive
			if connector.ChangeStreamSupported() {
				driver, yes := connector.(ChangeStreamDriver)
				if !yes {
					return fmt.Errorf("%s does not implement ChangeStreamDriver", connector.Type())
				}

				logger.Info("Starting ChangeStream process in driver")

				// Setup Global State from Connector
				if err := driver.SetupGlobalState(state); err != nil {
					return err
				}

				err := driver.RunChangeStream(pool, cdcStreams...)
				if err != nil {
					return fmt.Errorf("error occurred while reading records: %s", err)
				}
			}
			logger.Info("Read Process Completed")
			return nil
		})

		// Execute streams in Standard Stream mode
		// TODO: Separate streams with FULL and Incremental here only
		relec.ConcurrentInGroup(GlobalCxGroup, standardModeStreams, func(_ context.Context, stream Stream) error { // context is not used to keep processes mutually exclusive
			logger.Infof("Reading stream[%s] in %s", stream.ID(), stream.GetSyncMode())

			streamStartTime := time.Now()
			err := connector.Read(pool, stream)
			if err != nil {
				return fmt.Errorf("error occurred while reading records: %s", err)
			}

			logger.Infof("Finished reading stream %s[%s] in %s", stream.Name(), stream.Namespace(), time.Since(streamStartTime).String())

			return nil
		})

		if err := GlobalCxGroup.Block(); err != nil {
			return err
		}

		// wait for writer pool to finish
		if err := pool.Wait(); err != nil {
			return fmt.Errorf("error occurred in writer pool: %s", err)
		}

		logger.Infof("Total records read: %d", pool.TotalRecords())
		if !state.IsZero() {
			logger.LogState(state)
		}

		return nil
	},
}
