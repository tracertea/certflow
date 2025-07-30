package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/tracertea/certflow/internal/ctprocessor"
)

var (
	infoDir       string
	infoRecursive bool
)

var infoCmd = &cobra.Command{
	Use:   "info [file...]",
	Short: "Display information about CT data files",
	Long: `Display information about certificate transparency data files without processing them.
This command provides quick insights into file contents, line counts, and basic validation.

Examples:
  ctprocessor info data.gz
  ctprocessor info file1.gz file2.gz file3.gz
  ctprocessor info -d /path/to/ct-data/
  ctprocessor info -d /path/to/ct-data/ -r`,
	RunE: runInfo,
}

func runInfo(cmd *cobra.Command, args []string) error {
	logger := log.New(os.Stdout, "INFO: ", log.LstdFlags)
	sp := ctprocessor.NewStreamingProcessor(logger)

	fmt.Printf("Certificate Transparency File Information\n")
	fmt.Printf("========================================\n\n")

	// Determine input files
	var filePaths []string
	var err error

	if infoDir != "" {
		// Directory mode - scan for .gz files
		filePaths, err = scanDirectoryForGzipFiles(infoDir, infoRecursive, logger)
		if err != nil {
			return fmt.Errorf("failed to scan directory: %v", err)
		}
		if len(filePaths) == 0 {
			return fmt.Errorf("no .gz files found in directory: %s", infoDir)
		}
	} else if len(args) > 0 {
		// File arguments provided
		filePaths = args
	} else {
		return fmt.Errorf("either provide file arguments or use --input-dir")
	}

	totalFiles := len(filePaths)
	var totalCertificates int64
	successfulFiles := 0

	for i, filePath := range filePaths {
		fmt.Printf("File %d/%d: %s\n", i+1, totalFiles, filePath)
		
		// Get file info
		info, err := sp.GetFileInfo(filePath)
		if err != nil {
			fmt.Printf("  ❌ Error: %v\n\n", err)
			continue
		}

		fmt.Printf("  📊 Certificates: %d\n", info.LineCount)
		
		// Get file size
		if stat, err := os.Stat(filePath); err == nil {
			fmt.Printf("  💾 File size: %s\n", formatBytes(stat.Size()))
		}

		// Estimate processing time (rough calculation based on 100k certs in ~3 seconds)
		estimatedSeconds := float64(info.LineCount) / 100000.0 * 3.0
		fmt.Printf("  ⏱️  Estimated processing time: %s\n", formatDuration(estimatedSeconds))

		totalCertificates += info.LineCount
		successfulFiles++
		fmt.Println()
	}

	// Summary
	if successfulFiles > 0 {
		fmt.Printf("Summary\n")
		fmt.Printf("=======\n")
		fmt.Printf("Files processed: %d/%d\n", successfulFiles, totalFiles)
		fmt.Printf("Total certificates: %s\n", formatNumber(totalCertificates))
		
		if successfulFiles > 1 {
			avgCerts := totalCertificates / int64(successfulFiles)
			fmt.Printf("Average per file: %s\n", formatNumber(avgCerts))
		}
		
		totalEstimatedSeconds := float64(totalCertificates) / 100000.0 * 3.0
		fmt.Printf("Total estimated processing time: %s\n", formatDuration(totalEstimatedSeconds))
	}

	return nil
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatNumber(num int64) string {
	if num < 1000 {
		return fmt.Sprintf("%d", num)
	} else if num < 1000000 {
		return fmt.Sprintf("%.1fK", float64(num)/1000)
	} else {
		return fmt.Sprintf("%.1fM", float64(num)/1000000)
	}
}

func formatDuration(seconds float64) string {
	if seconds < 60 {
		return fmt.Sprintf("%.1fs", seconds)
	} else if seconds < 3600 {
		return fmt.Sprintf("%.1fm", seconds/60)
	} else {
		return fmt.Sprintf("%.1fh", seconds/3600)
	}
}

func init() {
	infoCmd.Flags().StringVarP(&infoDir, "input-dir", "d", "", "Input directory containing gzip files")
	infoCmd.Flags().BoolVarP(&infoRecursive, "recursive", "r", false, "Process directories recursively")
}