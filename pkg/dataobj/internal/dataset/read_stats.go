package dataset

import (
	"fmt"

	"github.com/dustin/go-humanize"
)

type DownloadStats struct {
	PrimaryColumnPages   int
	SecondaryColumnPages int
	PrimaryColumnBytes   uint64
	SecondaryColumnBytes uint64

	PrimaryColumnUncompressedBytes   uint64
	SecondaryColumnUncompressedBytes uint64
}

// ReadStats tracks statistics about dataset reading operations.
type ReadStats struct {
	PrimaryColumns   int
	SecondaryColumns int

	PrimaryColumnPages   uint64 // Total pages in primary columns
	SecondaryColumnPages uint64 // Total pages in secondary columns

	DownloadStats DownloadStats // Download statistics for primary and secondary columns

	MaxRows                uint64
	PredicateRowRangeTotal uint64

	PrimaryRowReads   uint64
	SecondaryRowReads uint64

	TotalPagesRead uint64 // Total pages read
}

// String implements fmt.Stringer and provides a clean summary of read statistics.
func (s *ReadStats) String() string {
	return fmt.Sprintf(`primary_columns=%d secondary_columns=%d
max_rows_per_column=%d rows_after_pruning=%d primary_rows_read=%d secondary_rows_read=%d
primary_column_pages=%d secondary_column_pages=%d total_pages_read=%d
primary_pages_downloaded=%d secondary_pages_downloaded=%d primary_bytes_downloaded=%s secondary_bytes_downloaded=%s primary_uncompressed_bytes_downloaded=%s secondary_uncompressed_bytes_downloaded=%s`,
		s.PrimaryColumns,
		s.SecondaryColumns,
		s.MaxRows,
		s.PredicateRowRangeTotal,
		s.PrimaryRowReads,
		s.SecondaryRowReads,
		s.PrimaryColumnPages,
		s.SecondaryColumnPages,
		s.TotalPagesRead,
		s.DownloadStats.PrimaryColumnPages,
		s.DownloadStats.SecondaryColumnPages,
		humanize.Bytes(s.DownloadStats.PrimaryColumnBytes),
		humanize.Bytes(s.DownloadStats.SecondaryColumnBytes),
		humanize.Bytes(s.DownloadStats.PrimaryColumnUncompressedBytes),
		humanize.Bytes(s.DownloadStats.SecondaryColumnUncompressedBytes),
	)
}
