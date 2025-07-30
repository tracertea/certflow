package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// scanDirectoryForGzipFiles scans a directory for .gz files
func scanDirectoryForGzipFiles(dirPath string, recursive bool, logger *log.Logger) ([]string, error) {
	var gzipFiles []string
	
	// Check if directory exists
	info, err := os.Stat(dirPath)
	if err != nil {
		return nil, fmt.Errorf("cannot access directory %s: %v", dirPath, err)
	}
	
	if !info.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", dirPath)
	}
	
	logger.Printf("Scanning directory: %s (recursive: %v)", dirPath, recursive)
	
	if recursive {
		// Walk directory tree recursively
		err = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				logger.Printf("Warning: Error accessing %s: %v", path, err)
				return nil // Continue walking
			}
			
			if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".gz") {
				gzipFiles = append(gzipFiles, path)
			}
			
			return nil
		})
	} else {
		// Scan only the specified directory (non-recursive)
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read directory %s: %v", dirPath, err)
		}
		
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(strings.ToLower(entry.Name()), ".gz") {
				fullPath := filepath.Join(dirPath, entry.Name())
				gzipFiles = append(gzipFiles, fullPath)
			}
		}
	}
	
	if err != nil {
		return nil, fmt.Errorf("error scanning directory: %v", err)
	}
	
	logger.Printf("Found %d .gz files in directory", len(gzipFiles))
	
	// Sort files for consistent processing order
	// Using a simple sort to ensure deterministic behavior
	for i := 0; i < len(gzipFiles)-1; i++ {
		for j := i + 1; j < len(gzipFiles); j++ {
			if gzipFiles[i] > gzipFiles[j] {
				gzipFiles[i], gzipFiles[j] = gzipFiles[j], gzipFiles[i]
			}
		}
	}
	
	return gzipFiles, nil
}