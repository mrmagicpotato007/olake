package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/fraugster/parquet-go/parquet"
	pqgo "github.com/parquet-go/parquet-go"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

type FileMetadata struct {
	fileName    string
	recordCount int
	writer      any
	file        source.ParquetFile
}

// Parquet destination writes Parquet files to a local path and optionally uploads them to S3.
type Parquet struct {
	options          *protocol.Options
	config           *Config
	partitionFolders map[string][]FileMetadata // path -> pqFile
	stream           protocol.Stream
	basePath         string
	baseFileName     *string
	pqSchemaMutex    sync.Mutex // To prevent concurrent map access from fraugster library
	s3Client         *s3.S3
}

// GetConfigRef returns the config reference for the parquet writer.
func (p *Parquet) GetConfigRef() protocol.Config {
	p.config = &Config{}
	return p.config
}

// Spec returns a new Config instance.
func (p *Parquet) Spec() any {
	return Config{}
}

// setup s3 client if credentials provided
func (p *Parquet) initS3Writer() error {
	if p.config.Bucket == "" || p.config.Region == "" {
		return nil
	}

	s3Config := aws.Config{
		Region: aws.String(p.config.Region),
	}
	if p.config.AccessKey != "" && p.config.SecretKey != "" {
		s3Config.Credentials = credentials.NewStaticCredentials(p.config.AccessKey, p.config.SecretKey, "")
	}
	sess, err := session.NewSession(&s3Config)
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %s", err)
	}
	p.s3Client = s3.New(sess)

	return nil
}

func (p *Parquet) createNewParquetWriter(dirPath string) error {
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories[%s]: %w", dirPath, err)
	}

	fileName := utils.TimestampedFileName(constants.ParquetFileExt)
	filePath := filepath.Join(dirPath, fileName)

	pqFile, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file writer: %w", err)
	}

	writer := func() any {
		if p.config.Normalization {
			return goparquet.NewFileWriter(pqFile,
				goparquet.WithSchemaDefinition(p.stream.Schema().ToParquet()),
				goparquet.WithMaxRowGroupSize(100),
				goparquet.WithMaxPageSize(10),
				goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
			)
		}
		return pqgo.NewGenericWriter[types.RawRecord](pqFile, pqgo.Compression(&pqgo.Snappy))
	}()

	p.partitionFolders[dirPath] = append(p.partitionFolders[dirPath], FileMetadata{
		fileName: fileName,
		file:     pqFile,
		writer:   writer,
	})

	// base file name if parition not there
	if p.baseFileName == nil {
		p.baseFileName = &fileName
	}
	return nil
}

// Setup configures the parquet writer, including local paths, file names, and optional S3 setup.
func (p *Parquet) Setup(stream protocol.Stream, options *protocol.Options) error {
	p.options = options
	p.stream = stream
	p.partitionFolders = make(map[string][]FileMetadata)

	// for s3 p.config.path may not be provided
	if p.config.Path == "" {
		p.config.Path = os.TempDir()
	}

	p.basePath = filepath.Join(p.config.Path, p.stream.Namespace(), p.stream.Name())
	err := p.createNewParquetWriter(p.basePath)
	if err != nil {
		return fmt.Errorf("failed to create new parquet writer for file[%s] : %s", p.baseFileName, err)
	}

	err = p.initS3Writer()
	if err != nil {
		return err
	}
	return nil
}

// Write writes a record to the Parquet file.
func (p *Parquet) Write(_ context.Context, record types.RawRecord) error {
	regexFilePath := p.getPartitionedFilePath(record.Data)

	partitionFolder, exists := p.partitionFolders[regexFilePath]
	if !exists {
		err := p.createNewParquetWriter(regexFilePath)
		if err != nil {
			return err
		}
		partitionFolder = p.partitionFolders[regexFilePath]
	}

	if len(partitionFolder) == 0 {
		return fmt.Errorf("failed to get partitioned files")
	}

	// get last written file
	fileMetadata := &partitionFolder[len(partitionFolder)-1]

	if p.config.Normalization {
		// TODO: Need to check if we can remove locking to fasten sync (Good First Issue)
		p.pqSchemaMutex.Lock()
		defer p.pqSchemaMutex.Unlock()
		if err := fileMetadata.writer.(*goparquet.FileWriter).AddData(record.Data); err != nil {
			return fmt.Errorf("parquet write error: %s", err)
		}
	} else {
		// locking not required as schema is fixed for base writer
		if _, err := fileMetadata.writer.(*pqgo.GenericWriter[types.RawRecord]).Write([]types.RawRecord{record}); err != nil {
			return fmt.Errorf("parquet write error: %s", err)
		}
	}
	fileMetadata.recordCount++
	return nil
}

// Check validates local paths and S3 credentials if applicable.
func (p *Parquet) Check() error {
	// check for s3 writer configuration
	err := p.initS3Writer()
	if err != nil {
		return err
	}
	// test for s3 permissions
	if p.s3Client != nil {
		testKey := fmt.Sprintf("olake_writer_test/%s", utils.TimestampedFileName(".txt"))
		// Try to upload a small test file
		_, err = p.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(testKey),
			Body:   strings.NewReader("S3 write test"),
		})
		if err != nil {
			return fmt.Errorf("failed to write test file to S3: %s", err)
		}
		p.config.Path = os.TempDir()
		logger.Info("s3 writer configuration found")
	} else if p.config.Path != "" {
		logger.Info("local writer configuration found, writing at location[%s]", p.config.Path)
	} else {
		return fmt.Errorf("invalid configuration found")
	}

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(p.config.Path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create path: %s", err)
	}

	// Test directory writability
	tempFile, err := os.CreateTemp(p.config.Path, "temporary-*.txt")
	if err != nil {
		return fmt.Errorf("directory is not writable: %s", err)
	}
	tempFile.Close()
	os.Remove(tempFile.Name())
	return nil
}

func (p *Parquet) Close() error {
	removeLocalFile := func(filePath, reason string, recordCount int) {
		err := os.Remove(filePath)
		if err != nil {
			logger.Warnf("Failed to delete file [%s] with %d records (%s): %s", filePath, recordCount, reason, err)
			return
		}
		logger.Debugf("Deleted file [%s] with %d records (%s).", filePath, recordCount, reason)
	}

	// Defer closing and possible file removal if no records were written
	defer func() {
		for path, openFiles := range p.partitionFolders {
			for _, fileMetadata := range openFiles {
				if fileMetadata.recordCount == 0 {
					removeLocalFile(filepath.Join(path, fileMetadata.fileName), "no records written", fileMetadata.recordCount)
				}
			}
		}
	}()

	// Close the writer and file
	if err := utils.ErrExecSequential(func() error {
		for dir, openedFiles := range p.partitionFolders {
			for _, fileMetadata := range openedFiles {
				path := filepath.Join(dir, fileMetadata.fileName)
				if p.config.Normalization {
					err := fileMetadata.writer.(*goparquet.FileWriter).Close()
					if err != nil {
						return fmt.Errorf("failed to close normalize writer: %s", err)
					}
				} else {
					err := fileMetadata.writer.(*pqgo.GenericWriter[types.RawRecord]).Close()
					if err != nil {
						return fmt.Errorf("failed to close base writer: %s", err)
					}
				}
				if err := fileMetadata.file.Close(); err != nil {
					return fmt.Errorf("failed to close file: %s", err)
				}
				if fileMetadata.recordCount > 0 {
					logger.Infof("Finished writing file [%s] with %d records.", path, fileMetadata.recordCount)
				}
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to close parquet writer: %s", err)
	}

	// Upload to S3 if configured and records exist
	if p.s3Client != nil {
		for dir, openedFiles := range p.partitionFolders {
			for _, fileMetadata := range openedFiles {
				path := filepath.Join(dir, fileMetadata.fileName)
				file, err := os.Open(path)
				if err != nil {
					return fmt.Errorf("failed to open local file for S3 upload: %s", err)
				}
				defer file.Close()

				basePath := filepath.Join(p.stream.Namespace(), p.stream.Name())
				if p.config.Prefix != "" {
					basePath = filepath.Join(p.config.Prefix, basePath)
				}
				s3KeyPath := filepath.Join(basePath, fileMetadata.fileName)
				_, err = p.s3Client.PutObject(&s3.PutObjectInput{
					Bucket: aws.String(p.config.Bucket),
					Key:    aws.String(s3KeyPath),
					Body:   file,
				})
				if err != nil {
					return fmt.Errorf("failed to upload file to S3 (bucket: %s, path: %s): %s", p.config.Bucket, s3KeyPath, err)
				}

				// remove local file after upload
				removeLocalFile(path, "uploaded to S3", fileMetadata.recordCount)
				logger.Infof("Successfully uploaded file to S3: s3://%s/%s", p.config.Bucket, s3KeyPath)
			}
		}
	}

	return nil
}

// EvolveSchema updates the schema based on changes.
func (p *Parquet) EvolveSchema(change, typeChange bool, _ map[string]*types.Property) error {
	// p.pqSchemaMutex.Lock()
	// defer p.pqSchemaMutex.Unlock()
	// TODO: Edge Cases need to think here 1. if file changing is of partition only
	// if typeChange || change {
	// 	// change base file for it
	// 	p.baseFileName = utils.TimestampedFileName(constants.ParquetFileExt)
	// 	if err := p.CreateNewParquetWriter(p.basePath, p.baseFileName); err != nil { // init new writer
	// 		return err
	// 	}
	// }
	// // Attempt to set the schema definition
	// if err := p.openPqWriters.SetSchemaDefinition(p.stream.Schema().ToParquet()); err != nil {
	// 	return fmt.Errorf("failed to set schema definition: %s", err)
	// }
	return nil
}

// Type returns the type of the writer.
func (p *Parquet) Type() string {
	return string(types.Parquet)
}

// Flattener returns a flattening function for records.
func (p *Parquet) Flattener() protocol.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func (p *Parquet) Normalization() bool {
	return p.config.Normalization
}

func (p *Parquet) getPartitionedFilePath(values map[string]any) string {
	// Regex to match placeholders like {date, hour, "fallback"}
	pattern := p.stream.Self().StreamMetadata.PartitionRegex
	if pattern == "" {
		return p.basePath
	}

	patternRegex := regexp.MustCompile(`\{([^}]+)\}`)

	// Replace placeholders
	result := patternRegex.ReplaceAllStringFunc(pattern, func(match string) string {
		trimmed := strings.Trim(match, "{}")
		regexVarBlock := strings.Split(trimmed, ",")

		var replacedParts []string
		for i, part := range regexVarBlock {
			part = strings.TrimSpace(strings.Trim(part, `"`))

			if val, exists := values[part]; exists && val != "" {
				replacedParts = append(replacedParts, val.(string))
				break
			} else if i == len(regexVarBlock)-1 {
				replacedParts = append(replacedParts, part)
			}
		}
		return strings.Join(replacedParts, "/")
	})

	return filepath.Join(p.basePath, strings.TrimSuffix(result, "/"))
}
func init() {
	protocol.RegisteredWriters[types.Parquet] = func() protocol.Writer {
		return new(Parquet)
	}
}
