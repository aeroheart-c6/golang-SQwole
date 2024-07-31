package assembler

import (
	"context"
	"fmt"
	"testing"

	"code.in.spdigital.sg/sp-digital/athena/db/pg"
	"code.in.spdigital.sg/sp-digital/athena/testutil"
	"code.in.spdigital.sg/sp-digital/gemini/api/internal/repository/orm"
	"github.com/stretchr/testify/require"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
)

func TestBulkInsert_Fields(t *testing.T) {
	tcs := map[string]struct {
	}{
		"success__boil_cols_complete":   {},
		"success__boil_cols_incomplete": {},
		"success__boil_cols_absent":     {},
		"success__not_struct":           {},
	}

	for _ = range tcs {

	}
}

func TestBulkInsert_SQL(t *testing.T) {
	tcs := map[string]struct {
	}{
		"success__data_present":    {},
		"success__data_absent":     {},
		"success__data_pointer":    {},
		"success__data_not_struct": {},
	}

	for _ = range tcs {

	}
}

func TestBulkInsert_BindQuery(t *testing.T) {
	type SampleTable struct {
		ID    int64  `boil:"id"`
		Col01 string `boil:"col_01"`
		Col02 string `boil:"col_02"`
		Col03 string `boil:"col_03"`
		Col04 string `boil:"col_04"`
		Col05 string `boil:"col_05"`
		Col06 string `boil:"col_06"`
		Col07 string `boil:"col_07"`
		Col08 string `boil:"col_08"`
		Col09 string `boil:"col_09"`
		Col10 string `boil:"col_10"`
		Col11 string `boil:"col_11"`
		Col12 string `boil:"col_12"`
		Col13 string `boil:"col_13"`
		Col14 string `boil:"col_14"`
	}
	tableName := "sample"
	createSQL := "" +
		"CREATE TABLE \"" + tableName + "\" (\n" +
		"    \"id\" BIGINT PRIMARY KEY,\n" +
		"    \"col_01\" TEXT,\n" +
		"    \"col_02\" TEXT,\n" +
		"    \"col_03\" TEXT,\n" +
		"    \"col_04\" TEXT,\n" +
		"    \"col_05\" TEXT,\n" +
		"    \"col_06\" TEXT,\n" +
		"    \"col_07\" TEXT,\n" +
		"    \"col_08\" TEXT,\n" +
		"    \"col_09\" TEXT,\n" +
		"    \"col_10\" TEXT,\n" +
		"    \"col_11\" TEXT,\n" +
		"    \"col_12\" TEXT,\n" +
		"    \"col_13\" TEXT,\n" +
		"    \"col_14\" TEXT\n" +
		");"

	tcs := map[string]struct {
		gvnRowsCount int
		expCount     int
	}{
		"success__1000_rows_inserted": {
			gvnRowsCount: 1000,
			expCount:     1,
		},
		"success__2000_rows_inserted": {
			gvnRowsCount: 2000,
			expCount:     1,
		},
		"success__65535_params_used": {
			gvnRowsCount: 5000,
			expCount:     2,
		},
	}

	for desc, tc := range tcs {
		t.Run(desc, func(t *testing.T) {
			testutil.WithTxDB(t, func(tx pg.BeginnerExecutor) {
				// Given
				ctx := context.Background()

				_, err := tx.ExecContext(ctx, createSQL)
				require.NoError(t, err)

				data := make([]*SampleTable, 0, tc.gvnRowsCount)
				for idx := 0; idx < tc.gvnRowsCount; idx++ {
					value := fmt.Sprintf("DataRow__%d", idx)
					data = append(data, &SampleTable{
						ID:    int64(idx),
						Col01: value,
						Col02: value,
						Col03: value,
						Col04: value,
						Col05: value,
						Col06: value,
						Col07: value,
						Col08: value,
						Col09: value,
						Col10: value,
						Col11: value,
						Col12: value,
						Col13: value,
						Col14: value,
					})
				}

				// When
				op, err := NewBulkInsert(
					data,
					tableName,
					[]string{
						"id",
						"col_01",
						"col_02",
						"col_03",
						"col_04",
						"col_05",
						"col_06",
						"col_07",
						"col_08",
						"col_09",
						"col_10",
						"col_11",
						"col_12",
						"col_13",
						"col_14",
					},
				)
				require.NoError(t, err)

				groups, err := op.Queries()
				require.NoError(t, err)
				require.Equal(t, tc.expCount, len(groups))

				for _, group := range groups {
					batch := make([]*SampleTable, 0, group.DataEnd-group.DataStart)
					err = group.Query.Bind(ctx, tx, &batch)
					require.NoError(t, err)

					copy(data[group.DataStart:group.DataEnd], batch)
				}

				result := struct {
					Total int `boil:"total"`
				}{}

				checkQuery := orm.NewQuery(
					qm.Select("COUNT(1) AS total"),
					qm.From(tableName),
				)
				err = checkQuery.Bind(ctx, tx, &result)
				require.NoError(t, err)
				require.Equal(t, tc.gvnRowsCount, result.Total)
			})
		})
	}
}
