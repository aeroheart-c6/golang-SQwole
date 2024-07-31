package assembler

import (
	"fmt"
	"math"
	"reflect"
	"time"

	pkgerrors "github.com/pkg/errors"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
)

const psqlMaxParamCount = math.MaxUint16

// QueryGroup represents arguments needed for the batch query
type QueryGroup struct {
	Rows      []string
	Args      []interface{}
	DataStart int
	DataEnd   int
	Query     *queries.Query
}

// GetCurrentTime returns the current time but in the context of how SQLBoiler is configured to make it consistent in
//                the database
func GetCurrentTime() time.Time {
	return time.Now().In(boil.GetLocation())
}

// getBatchingInfo determine how many batches for bulk statements will be prepared based on the number of data involved
//                 and the prevailing limit of parameters per statement. See `psqMaxParamCount`. This limit is
//                 parameterized just for testing purposes.
func getBatchingInfo(dataCount int, fieldsCount int, limit int) (int, int) {
	batchLen := dataCount
	batchCount := 1

	paramsLen := dataCount * fieldsCount
	if paramsLen > limit {
		batchLen = int(math.Floor(float64(limit) / float64(fieldsCount)))
		batchCount = int(math.Ceil(float64(dataCount) / float64(batchLen)))
	}

	return batchLen, batchCount
}

// getStructFields gets field names of the struct based on the database column name -- this should base from the
//                     `boil` metatdata of the struct
func getStructFields(objValue reflect.Value, columns []string) ([]string, error) {
	if objValue.Kind() == reflect.Ptr {
		objValue = objValue.Elem()
	}

	if objValue.Kind() != reflect.Struct {
		return nil, pkgerrors.WithStack(ErrDataNotStruct)
	}

	// create the mapping
	objType := objValue.Type()
	fieldsCount := objValue.NumField()
	mapping := make(map[string]string, fieldsCount)

	for idx := 0; idx < fieldsCount; idx++ {
		field := objType.Field(idx)
		column := field.Tag.Get("boil")

		if column == "" || column == "-" {
			continue
		}

		mapping[column] = field.Name
	}

	fields := make([]string, 0, fieldsCount)
	// create the list of struct fields
	for _, column := range columns {
		field, found := mapping[column]
		if found {
			fields = append(fields, field)
		}
	}

	return fields, nil
}

// isSupportedType checks if the data passed is a valid array or slice data type. Returns a nil `reflect.Type` instance
//                 if the parameter is not an array or slice.
//
//                 who is that var I see
//                 staring straight back at me?
//                 how will my reflection show
//                 who is var inside?
func isSupportedType(data interface{}) (reflect.Type, reflect.Value, bool) {
	dataValue := reflect.ValueOf(data)
	dataType := dataValue.Type()
	dataKind := dataType.Kind()

	if dataKind == reflect.Ptr {
		dataValue = dataValue.Elem()
		dataType = dataValue.Type()
		dataKind = dataType.Kind()
	}

	if dataKind != reflect.Array && dataKind != reflect.Slice {
		return nil, reflect.Value{}, false
	}

	return dataType, dataValue, true
}

// quoteNames just adds double quotes to the names in case fields end up sounding like reserved words
func quoteNames(names []string) []string {
	output := make([]string, 0, len(names))
	for _, object := range names {
		output = append(output, fmt.Sprintf("\"%s\"", object))
	}

	return output
}
