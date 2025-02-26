package types

import (
	"fmt"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/goccy/go-json"
)

type DataType string

const (
	Null           DataType = "null"
	Int64          DataType = "integer"
	Float64        DataType = "number"
	String         DataType = "string"
	Bool           DataType = "boolean"
	Object         DataType = "object"
	Array          DataType = "array"
	Unknown        DataType = "unknown"
	Timestamp      DataType = "timestamp"
	TimestampMilli DataType = "timestamp_milli" // storing datetime up to 3 precisions
	TimestampMicro DataType = "timestamp_micro" // storing datetime up to 6 precisions
	TimestampNano  DataType = "timestamp_nano"  // storing datetime up to 9 precisions
)

type Record map[string]any

type RawRecord struct {
	OlakeID        string         `parquet:"olake_id"`
	Data           map[string]any `parquet:"data,json"`
	DeleteTime     int64          `parquet:"cdc_deleted_at"`
	OlakeTimestamp int64          `parquet:"olake_insert_time"`
	OperationType  string         `parquet:"_"`
	CdcTimestamp   int64          `parquet:"_"`
}

func (r *RawRecord) GetDebeziumJSON(db string, stream string) (string, error) {
	// First create the schema and track field types
	schema := r.createDebeziumSchema(db, stream)

	// Create the payload with the actual data
	payload := make(map[string]interface{})

	// Add olake_id to payload
	payload["olake_id"] = r.OlakeID

	// Copy the data fields
	for key, value := range r.Data {
		payload[key] = value
	}

	// Add the metadata fields
	payload["__deleted"] = r.DeleteTime > 0
	payload["__op"] = r.OperationType // "r" for read/backfill, "c" for create, "u" for update
	payload["__db"] = db
	payload["__source_ts_ms"] = r.CdcTimestamp

	// Create Debezium format
	debeziumRecord := map[string]interface{}{
		"destination_table": stream,
		"key": map[string]interface{}{
			"schema": map[string]interface{}{
				"type": "struct",
				"fields": []map[string]interface{}{
					{
						"type":     "string",
						"optional": true,
						"field":    "olake_id",
					},
				},
				"optional": false,
			},
			"payload": map[string]interface{}{
				"olake_id": r.OlakeID,
			},
		},
		"value": map[string]interface{}{
			"schema":  schema,
			"payload": payload,
		},
	}

	jsonBytes, err := json.Marshal(debeziumRecord)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func (r *RawRecord) createDebeziumSchema(db string, stream string) map[string]interface{} {
	fields := make([]map[string]interface{}, 0)

	// Add olake_id field first
	fields = append(fields, map[string]interface{}{
		"type":     "string",
		"optional": true,
		"field":    "olake_id",
	})

	// Add data fields
	for key, value := range r.Data {
		field := map[string]interface{}{
			"optional": true,
			"field":    key,
		}

		// Determine type based on the value
		switch value.(type) {
		case bool:
			field["type"] = "boolean"
		case int, int8, int16, int32:
			field["type"] = "int32"
		case int64:
			field["type"] = "int64"
		case float32:
			field["type"] = "float32"
		case float64:
			field["type"] = "float64"
		case map[string]interface{}:
			field["type"] = "string"
		case []interface{}:
			field["type"] = "string"
		default:
			field["type"] = "string"
		}

		fields = append(fields, field)
	}

	// Add metadata fields
	fields = append(fields, []map[string]interface{}{
		{
			"type":     "boolean",
			"optional": true,
			"field":    "__deleted",
		},
		{
			"type":     "string",
			"optional": true,
			"field":    "__op",
		},
		{
			"type":     "string",
			"optional": true,
			"field":    "__db",
		},
		{
			"type":     "int64",
			"optional": true,
			"field":    "__source_ts_ms",
		},
	}...)

	return map[string]interface{}{
		"type":     "struct",
		"fields":   fields,
		"optional": false,
		"name":     fmt.Sprintf("%s.%s", db, stream),
	}
}

func CreateRawRecord(olakeID string, data map[string]any, deleteAt int64, operationType string, cdcTimestamp int64) RawRecord {
	return RawRecord{
		OlakeID:       olakeID,
		Data:          data,
		OperationType: operationType,
		CdcTimestamp:  cdcTimestamp,
	}
}

// returns parquet equivalent type & convertedType for the datatype
func (d DataType) ToParquet() *parquet.SchemaElement {
	switch d {
	case Int64:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case Float64:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_DOUBLE),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case String:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BYTE_ARRAY),
			ConvertedType:  ToPointer(parquet.ConvertedType_UTF8),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case Bool:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BOOLEAN),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	//TODO: Not able to generate correctly in parquet, handle later
	case Timestamp:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case TimestampMilli:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	//TODO: Not able to generate correctly in parquet, handle later
	case TimestampNano:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case Object, Array: // Objects/Arrays are turned into String in parquet
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BYTE_ARRAY),
			ConvertedType:  ToPointer(parquet.ConvertedType_UTF8),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	default:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BYTE_ARRAY),
			ConvertedType:  ToPointer(parquet.ConvertedType_JSON),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	}
}
