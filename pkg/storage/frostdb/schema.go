package frostdb

import (
	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

const (
	ColumnLabels         = "labels"
	ColumnExemplarLabels = "exemplar_labels"
	ColumnTimestamp      = "timestamp"
	ColumnValue          = "value"
)

func exemplarSchema() (*dynparquet.Schema, error) {
	return dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "exemplars_schema",
		Columns: []*schemapb.Column{
			{
				Name: ColumnLabels,
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
					Nullable: true,
				},
				Dynamic: true,
			},
			{
				Name: ColumnExemplarLabels,
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
					Nullable: true,
				},
				Dynamic: true,
			},
			{
				Name: ColumnTimestamp,
				StorageLayout: &schemapb.StorageLayout{
					Type: schemapb.StorageLayout_TYPE_INT64,
				},
				Dynamic: false,
			},
			{
				Name: ColumnValue,
				StorageLayout: &schemapb.StorageLayout{
					Type: schemapb.StorageLayout_TYPE_DOUBLE,
				},
				Dynamic: false,
			},
		},
		SortingColumns: []*schemapb.SortingColumn{
			{
				Name:      ColumnLabels,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
			{
				Name:      ColumnExemplarLabels,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
			{
				Name:      ColumnTimestamp,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
		},
	})
}
