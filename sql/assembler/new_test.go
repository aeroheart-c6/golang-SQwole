package assembler

import (
	"testing"

	"code.in.spdigital.sg/sp-digital/athena/db/pg"
	"code.in.spdigital.sg/sp-digital/athena/testutil"
	"code.in.spdigital.sg/sp-digital/gemini/api/internal/model"
	"code.in.spdigital.sg/sp-digital/gemini/api/internal/repository/orm"
	"github.com/stretchr/testify/require"
)

func TestNewBulkInsert(t *testing.T) {
	tcs := map[string]struct {
		gvnSubstations []*orm.Substation
		gvnInts        []int
		gvnPassSS      bool
		gvnPassSSSlice bool
		expErr         error
	}{
		"success__nonempty_array": {
			gvnSubstations: []*orm.Substation{
				{
					ID:           1,
					AssetID:      "DXSS0001",
					MRC:          "DXSS0001",
					Name:         "Substation 0001",
					NetworkType:  model.NetworkDX.String(),
					Zone:         model.ZoneNorth.String(),
					Status:       model.AssetStatusCommissioned.String(),
					VoltageClass: model.VoltageClassificationA1.String(),
				},
			},
			gvnInts:        []int{1, 2, 3},
			gvnPassSS:      true,
			gvnPassSSSlice: false,
		},
		"success__nonempty_slice": {
			gvnSubstations: []*orm.Substation{
				{
					ID:           1,
					AssetID:      "DXSS0001",
					MRC:          "DXSS0001",
					Name:         "Substation 0001",
					NetworkType:  model.NetworkDX.String(),
					Zone:         model.ZoneNorth.String(),
					Status:       model.AssetStatusCommissioned.String(),
					VoltageClass: model.VoltageClassificationA1.String(),
				},
			},
			gvnInts:        nil,
			gvnPassSS:      true,
			gvnPassSSSlice: true,
		},
		"failure__empty_array": {
			gvnSubstations: []*orm.Substation{},
			gvnInts:        nil,
			gvnPassSS:      true,
			gvnPassSSSlice: true,
			expErr:         ErrDataEmpty,
		},
		"failure__nil_array": {
			gvnSubstations: nil,
			gvnInts:        nil,
			gvnPassSS:      true,
			gvnPassSSSlice: false,
			expErr:         ErrDataEmpty,
		},
		"failure__invalid_type": {
			gvnSubstations: nil,
			gvnInts:        []int{1, 2, 3},
			gvnPassSS:      false,
			gvnPassSSSlice: false,
			expErr:         ErrDataNotStruct,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			testutil.WithTxDB(t, func(tx pg.BeginnerExecutor) {
				// GIVEN
				var (
					data []*orm.Substation
					op   BulkInsert
					err  error
				)

				if tc.gvnPassSSSlice {
					data = tc.gvnSubstations[:]
				} else {
					data = tc.gvnSubstations
				}

				columns := []string{
					orm.SubstationColumns.ID,
				}

				// WHEN
				if tc.gvnPassSS {
					op, err = NewBulkInsert(data, orm.TableNames.Substations, columns)
				} else {
					op, err = NewBulkInsert(tc.gvnInts, orm.TableNames.Substations, columns)
				}

				// THEN
				if tc.expErr != nil {
					require.EqualError(t, err, tc.expErr.Error())
					return
				}

				require.NoError(t, err)
				require.Equal(t, op.Columns, columns)
			})
		})
	}
}

func TestNewBulkUpsert(t *testing.T) {
	tcs := map[string]struct {
		gvnSubstations []*orm.Substation
		gvnInts        []int
		gvnPassSS      bool
		gvnPassSSSlice bool
		gvnColInsert   []string
		gvnColUpdate   []string
		expErr         error
	}{
		"success__nonempty_array": {
			gvnSubstations: []*orm.Substation{
				{
					ID:           1,
					AssetID:      "DXSS0001",
					MRC:          "DXSS0001",
					Name:         "Substation 0001",
					NetworkType:  model.NetworkDX.String(),
					Zone:         model.ZoneNorth.String(),
					Status:       model.AssetStatusCommissioned.String(),
					VoltageClass: model.VoltageClassificationA1.String(),
				},
			},
			gvnInts:        []int{1, 2, 3},
			gvnPassSS:      true,
			gvnPassSSSlice: false,
			gvnColInsert:   []string{orm.SubstationColumns.ID},
			gvnColUpdate:   []string{orm.SubstationColumns.ID, orm.SubstationColumns.AssetID},
		},
		"success__nonempty_slice": {
			gvnSubstations: []*orm.Substation{
				{
					ID:           1,
					AssetID:      "DXSS0001",
					MRC:          "DXSS0001",
					Name:         "Substation 0001",
					NetworkType:  model.NetworkDX.String(),
					Zone:         model.ZoneNorth.String(),
					Status:       model.AssetStatusCommissioned.String(),
					VoltageClass: model.VoltageClassificationA1.String(),
				},
			},
			gvnInts:        nil,
			gvnPassSS:      true,
			gvnPassSSSlice: true,
			gvnColInsert:   []string{orm.SubstationColumns.ID},
			gvnColUpdate:   []string{orm.SubstationColumns.ID, orm.SubstationColumns.AssetID},
		},
		"success__update_columns_fallback": {
			gvnSubstations: []*orm.Substation{
				{
					ID:           1,
					AssetID:      "DXSS0001",
					MRC:          "DXSS0001",
					Name:         "Substation 0001",
					NetworkType:  model.NetworkDX.String(),
					Zone:         model.ZoneNorth.String(),
					Status:       model.AssetStatusCommissioned.String(),
					VoltageClass: model.VoltageClassificationA1.String(),
				},
			},
			gvnInts:        nil,
			gvnPassSS:      true,
			gvnPassSSSlice: true,
			gvnColInsert:   []string{orm.SubstationColumns.ID},
			gvnColUpdate:   nil,
		},
		"failure__empty_array": {
			gvnSubstations: []*orm.Substation{},
			gvnInts:        nil,
			gvnPassSS:      true,
			gvnPassSSSlice: true,
			expErr:         ErrDataEmpty,
		},
		"failure__nil_array": {
			gvnSubstations: nil,
			gvnInts:        nil,
			gvnPassSS:      true,
			gvnPassSSSlice: false,
			expErr:         ErrDataEmpty,
		},
		"failure__invalid_type": {
			gvnSubstations: nil,
			gvnInts:        []int{1, 2, 3},
			gvnPassSS:      false,
			gvnPassSSSlice: false,
			expErr:         ErrDataNotStruct,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			testutil.WithTxDB(t, func(tx pg.BeginnerExecutor) {
				// GIVEN
				var (
					data []*orm.Substation
					op   BulkUpsert
					err  error
				)

				if tc.gvnPassSSSlice {
					data = tc.gvnSubstations[:]
				} else {
					data = tc.gvnSubstations
				}

				conflicts := []string{orm.SubstationColumns.AssetID}

				// WHEN
				if tc.gvnPassSS {
					op, err = NewBulkUpsert(
						data,
						orm.TableNames.Substations,
						conflicts,
						tc.gvnColInsert,
						tc.gvnColUpdate,
					)
				} else {
					op, err = NewBulkUpsert(
						tc.gvnInts,
						orm.TableNames.Substations,
						conflicts,
						tc.gvnColInsert,
						tc.gvnColUpdate,
					)
				}

				// THEN
				if tc.expErr != nil {
					require.EqualError(t, err, tc.expErr.Error())
					return
				}

				require.NoError(t, err)
				require.Equal(t, op.ConflictTargets, conflicts)
				require.Equal(t, op.Columns, tc.gvnColInsert)

				if tc.gvnColUpdate == nil {
					require.Equal(t, op.ColumnsUpdate, tc.gvnColInsert)
				} else {
					require.Equal(t, op.ColumnsUpdate, tc.gvnColUpdate)
				}
			})
		})
	}
}
