package constants

const (
	ParquetFileExt       = "parquet"
	MongoPrimaryID       = "_id"
	MongoPrimaryIDPrefix = `ObjectID("`
	MongoPrimaryIDSuffix = `")`
	OlakeID              = "olake_id"
	OlakeTimestamp       = "olake_insert_time"
	CDCUpdatedAt         = "_cdc_updated_at"
	CDCDeletedAt         = "_cdc_deleted_at"
	CDCLSN               = "_cdc_lsn"
)
