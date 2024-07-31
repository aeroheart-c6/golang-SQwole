package assembler

import (
	"math"
	"reflect"
	"strings"

	boilQueries "github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/strmangle"
)

/**
 * Question:
 * why does SQL boiler need to do a cache key? and why need to lock?
 *      -- lock is with the `sync` package ... want to make caching thread safe
 *      -- but what is being cached?
 *          -- the mapping of columns and struct
 *          -- SQL
 *
 * we can't take advantage of boil.Infer() because generated variables like:
 * 		- xxxxAllColumns
 *		- xxxxColumnsWithoutDefault
 *		- xxxxColumnsWithDefault
 * 		- xxxxPrimaryKeyColumns
 *		- xxxxGeneratedColumns
 * are not exported, thus, cannot be accessed by code outside the `orm` package
 *
 * therefore:
 *     - column specificiation policy: always whitelist
 *     - no caching needed
 *
 * for `RETURNING` clause, I want to keep it simple -- if it's involved in the query, return it.
 */

// BulkInsert represents an assembler for bulk insert SQL
type BulkInsert struct {
	Data      interface{}
	DataType  reflect.Type
	DataValue reflect.Value
	Table     string
	Columns   []string
}

// Fields returns the list of struct fields that are annotated as database ORM fields
func (op BulkInsert) Fields() ([]string, error) {
	return getStructFields(
		op.DataValue.Index(0),
		op.Columns,
	)
}

// Queries returns the built SQL statement as a SQLBoiler `queries.Query` object
func (op BulkInsert) Queries() ([]QueryGroup, error) {
	groups, err := op.sqlData()
	if err != nil {
		return nil, err
	}

	for idx := range groups {
		group := &groups[idx]
		group.Query = boilQueries.Raw(
			op.sqlStatement(*group),
			group.Args...,
		)
	}

	return groups, nil
}

// SQL builds the raw SQL that can be easily passed to SQLBoiler's APIs
func (op BulkInsert) sqlStatement(group QueryGroup) string {
	cols := strings.Join(quoteNames(op.Columns), ",")
	sql := "" +
		"INSERT INTO \"" + op.Table + "\" (" + cols + ")\n" +
		"VALUES\n" +
		strings.Join(group.Rows, ",\n") + "\n" +
		"RETURNING (" + cols + ")"

	return sql
}

// sqlData extracts values from the array of structs. For `orm.*` structs, there seem to be no pointers generated who
//         instead represented with a `null.*` counterpart.
func (op BulkInsert) sqlData() ([]QueryGroup, error) {
	fields, err := op.Fields()
	if err != nil {
		return nil, err
	}

	fieldsCount := len(fields)
	valueLen := op.DataValue.Len()
	batchLen, batchCnt := getBatchingInfo(valueLen, fieldsCount, psqlMaxParamCount)

	groups := make([]QueryGroup, 0, batchCnt)
	for batchIdx := 0; batchIdx < batchCnt; batchIdx++ {
		rows := make([]string, 0, batchLen)
		args := make([]interface{}, 0, batchLen)
		idxBase := batchLen * batchIdx

		// if it's the last batch, it will likely be shorter than `batchLen`
		limit := batchLen
		if batchIdx >= batchCnt-1 {
			limit = int(math.Min(float64(batchLen), float64(valueLen-batchIdx*batchLen)))
		}

		for rowIdx := 0; rowIdx < limit; rowIdx++ {
			idx := rowIdx + idxBase
			row := op.DataValue.Index(idx)

			// if we got passed an array of pointers to `orm.*` struct
			if row.Kind() == reflect.Ptr {
				row = row.Elem()
			}

			for _, field := range fields {
				args = append(args, row.FieldByName(field).Interface())
			}

			// psql placeholders are numbered vs mysql's "?"
			rows = append(rows, strmangle.Placeholders(true, fieldsCount, fieldsCount*rowIdx+1, fieldsCount))
		}

		groups = append(groups, QueryGroup{
			Rows:      rows,
			Args:      args,
			DataStart: idxBase,
			DataEnd:   idxBase + limit,
		})
	}

	return groups, nil
}
