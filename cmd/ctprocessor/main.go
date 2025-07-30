package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "1.0.0"
	commit  = "dev"
	date    = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "ctprocessor",
	Short: "Certificate Transparency Processor",
	Long: `Certificate Transparency Processor (ctprocessor) is a high-performance tool for processing 
certificate transparency data from gzip files and extracting domain-to-organization mappings.

Features:
  - Process large CT datasets (100k+ certificates)
  - Process CT data directly from AWS S3 buckets
  - Concurrent processing with configurable worker pools
  - Multiple output formats (JSON, CSV, text)
  - Comprehensive error handling and recovery
  - Streaming processing for memory efficiency
  - Batch processing for multiple files
  - State tracking for resumable S3 processing

Examples:
  ctprocessor process -i data.gz -o output.json
  ctprocessor process-s3 --bucket my-ct-bucket -o output.json
  ctprocessor info data.gz
  ctprocessor version`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Certificate Transparency Processor v%s\n", version)
		fmt.Println("Use 'ctprocessor --help' for available commands")
		fmt.Println("Use 'ctprocessor process --help' for processing options")
	},
}

func init() {
	rootCmd.AddCommand(processCmd)
	rootCmd.AddCommand(processS3Cmd)
	rootCmd.AddCommand(infoCmd)
	rootCmd.AddCommand(versionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}