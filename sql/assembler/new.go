package assembler

import (
	"reflect"

	pkgerrors "github.com/pkg/errors"
)

/**
 * Question: how do we limit the length of the SQL?
 * Answer: lol we don't since the SQL limit is high enough (2GB) that it makes little sense to keep the entire data in
 *         in-memory. furthermore, too much unnecessary logic needed:
 *             > quoting names
 *             > quoting values
 *             > substituting the SQL parameters (?) into actual values
 *             > counting SQL length post-quoting before adding a specific row post-quoting
 *
 *
 * Question: does this library also autopopulate CreatedAt and UpdatedAt fields?
 * Answer: no. If you need this library, you're going to have to write it yourself. I am not sure if this is in scope of
 *         an SQL assembler-only package. SQLBoiler does it through the generated code.
 *
 *         In fact, generating the `queries.Query` object in this library is already kinda sus.
 */

// NewBulkInsert creates a new instance that will help assemble a bulk INSERT SQL for Postgres
func NewBulkInsert(
	data interface{},
	table string,
	columns []string,
) (BulkInsert, error) {
	dataType, dataValue, ok := isSupportedType(data)

	if !ok {
		return BulkInsert{}, pkgerrors.WithStack(ErrDataNotArray)
	}

	if dataValue.Len() <= 0 {
		return BulkInsert{}, pkgerrors.WithStack(ErrDataEmpty)
	}

	item := dataValue.Index(0)
	if item.Kind() == reflect.Ptr {
		item = item.Elem()
	}
	if item.Kind() != reflect.Struct {
		return BulkInsert{}, pkgerrors.WithStack(ErrDataNotStruct)
	}

	return BulkInsert{
		Data:      data,
		DataType:  dataType,
		DataValue: dataValue,
		Table:     table,
		Columns:   columns,
	}, nil
}

// NewBulkUpsert creates a new instance that will help assemble a bulk INSERT ON CONFLICT SQL for Postgres
func NewBulkUpsert(
	data interface{},
	table string,
	conflicts []string,
	columnsInsert []string,
	columnsUpdate []string,
) (BulkUpsert, error) {
	dataType, dataValue, ok := isSupportedType(data)

	if !ok {
		return BulkUpsert{}, pkgerrors.WithStack(ErrDataNotArray)
	}

	if dataValue.Len() <= 0 {
		return BulkUpsert{}, pkgerrors.WithStack(ErrDataEmpty)
	}

	item := dataValue.Index(0)
	if item.Kind() == reflect.Ptr {
		item = item.Elem()
	}
	if item.Kind() != reflect.Struct {
		return BulkUpsert{}, pkgerrors.WithStack(ErrDataNotStruct)
	}

	if columnsUpdate == nil {
		columnsUpdate = columnsInsert
	}

	return BulkUpsert{
		BulkInsert: BulkInsert{
			Data:      data,
			DataType:  dataType,
			DataValue: dataValue,
			Table:     table,
			Columns:   columnsInsert,
		},
		ColumnsUpdate:   columnsUpdate,
		ConflictTargets: conflicts,
	}, nil
}
