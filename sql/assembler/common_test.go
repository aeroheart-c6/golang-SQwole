package assembler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommon_getBatchingInfo(t *testing.T) {
	tcs := map[string]struct {
		gvnDataCount   int
		gvnFieldsCount int
		gvnArgsLimit   int
		expLength      int
		expCount       int
	}{
		"success__15fields_5000rows_75000params": {
			gvnDataCount:   5000,
			gvnFieldsCount: 15,
			gvnArgsLimit:   psqlMaxParamCount,
			expLength:      4369,
			expCount:       2,
		},
	}

	for desc, tc := range tcs {
		t.Run(desc, func(t *testing.T) {
			// When
			length, count := getBatchingInfo(
				tc.gvnDataCount,
				tc.gvnFieldsCount,
				tc.gvnArgsLimit,
			)

			// Then
			require.Equal(t, tc.expLength, length)
			require.Equal(t, tc.expCount, count)
		})
	}
}
