package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Display version information",
	Long:  `Display detailed version information including build details and runtime information.`,
	Run:   runVersion,
}

func runVersion(cmd *cobra.Command, args []string) {
	fmt.Printf("Certificate Transparency Processor\n")
	fmt.Printf("==================================\n\n")
	
	fmt.Printf("Version:    %s\n", version)
	fmt.Printf("Commit:     %s\n", commit)
	fmt.Printf("Build Date: %s\n", date)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("Platform:   %s/%s\n", runtime.GOOS, runtime.GOARCH)
	
	fmt.Printf("\nFeatures:\n")
	fmt.Printf("  ✓ Concurrent processing with worker pools\n")
	fmt.Printf("  ✓ Multiple output formats (JSON, CSV, text)\n")
	fmt.Printf("  ✓ Streaming processing for large datasets\n")
	fmt.Printf("  ✓ Comprehensive error handling\n")
	fmt.Printf("  ✓ Batch processing for multiple files\n")
	fmt.Printf("  ✓ Memory-efficient gzip decompression\n")
	fmt.Printf("  ✓ X.509 certificate parsing\n")
	fmt.Printf("  ✓ Domain-to-organization mapping\n")
}