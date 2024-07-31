package assembler

import (
	"fmt"
	"strings"

	"github.com/volatiletech/sqlboiler/v4/queries"
)

// BulkUpsert represents an assembler for bulk upsert SQL
type BulkUpsert struct {
	BulkInsert
	ColumnsUpdate   []string
	ConflictTargets []string
}

// Queries returns the built SQL statement as a SQLBoiler `queries.Query` object. Overridden because our `call to `sqlStatement()`
//         is overridden, and we want to call `BulkUpsert`'s rather than `BulkInsert`'s
func (op BulkUpsert) Queries() ([]QueryGroup, error) {
	groups, err := op.sqlData()
	if err != nil {
		return nil, err
	}

	for idx := range groups {
		group := &groups[idx]
		group.Query = queries.Raw(
			op.sqlStatement(*group),
			group.Args...,
		)
	}

	return groups, nil
}

// SQL builds the raw SQL and the corresponding arguments that can be easily passed to SQLBoiler's APIs
func (op BulkUpsert) sqlStatement(group QueryGroup) string {
	updates := make([]string, 0, len(op.ColumnsUpdate))
	for _, column := range op.ColumnsUpdate {
		updates = append(updates, fmt.Sprintf("    \"%[1]s\" = \"excluded\".\"%[1]s\"", column))
	}

	colsUpdate := strings.Join(updates, ",\n")
	colsConflict := strings.Join(quoteNames(op.ConflictTargets), ",")
	cols := strings.Join(quoteNames(op.Columns), ",")
	rows := strings.Join(group.Rows, ",\n")
	sql := "" +
		"INSERT INTO \"" + op.Table + "\" (" + cols + ")\n" +
		"VALUES\n" +
		rows + "\n" +
		"ON CONFLICT (" + colsConflict + ")\n" +
		"DO UPDATE SET\n" +
		colsUpdate + "\n" +
		"RETURNING (" + cols + ")"

	return sql
}
