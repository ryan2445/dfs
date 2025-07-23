package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"time"
)

const (
	benchmarkFileSize = 2 * 1024 * 1024 * 1024 // 2GB
	testFileName      = "benchmark_test_file.bin"
	downloadFileName  = "benchmark_downloaded_file.bin"
)

type BenchmarkResult struct {
	Operation   string
	Duration    time.Duration
	FileSize    int64
	Throughput  float64 // MB/s
	MemoryUsage runtime.MemStats
	CPUUsage    float64
}

func main() {
	defer func() {
		fmt.Println("Performing cleanup...")
		if err := os.Remove(testFileName); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: Failed to remove %s: %v\n", testFileName, err)
		}
		if err := os.Remove(downloadFileName); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: Failed to remove %s: %v\n", downloadFileName, err)
		}
		fmt.Println("Cleanup complete")
	}()

	fmt.Println("=== DFS Benchmark Test ===")
	fmt.Printf("File size: %.2f GB\n", float64(benchmarkFileSize)/(1024*1024*1024))
	fmt.Println()

	// Generate test file
	fmt.Println("1. Generating 2GB test file with random data...")
	generateStart := time.Now()
	if err := generateTestFile(testFileName, benchmarkFileSize); err != nil {
		log.Fatalf("Failed to generate test file: %v", err)
	}
	generateDuration := time.Since(generateStart)
	fmt.Printf("   Generated in %.2fs (%.2f MB/s)\n",
		generateDuration.Seconds(),
		float64(benchmarkFileSize)/(1024*1024)/generateDuration.Seconds())
	fmt.Println()

	// Upload benchmark
	fmt.Println("2. Starting upload benchmark...")
	uploadResult, err := benchmarkUpload(testFileName)
	if err != nil {
		log.Fatalf("Upload benchmark failed: %v", err)
	}
	printBenchmarkResult(uploadResult)
	fmt.Println()

	// Download benchmark
	fmt.Println("3. Starting download benchmark...")
	downloadResult, err := benchmarkDownload(testFileName, downloadFileName)
	if err != nil {
		log.Fatalf("Download benchmark failed: %v", err)
	}
	printBenchmarkResult(downloadResult)
	fmt.Println()

	// Verify file integrity
	fmt.Println("4. Verifying file integrity...")
	if err := verifyFiles(testFileName, downloadFileName); err != nil {
		log.Fatalf("File verification failed: %v", err)
	}
	fmt.Println("   Files match - integrity verified!")
	fmt.Println()

	fmt.Println("=== Benchmark Complete ===")
}

func generateTestFile(filename string, size int64) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Generate random data in chunks to avoid memory issues
	const chunkSize = 32 * 1024 * 1024 // 32MB chunks
	written := int64(0)

	for written < size {
		remaining := size - written
		currentChunkSize := chunkSize
		if remaining < int64(chunkSize) {
			currentChunkSize = int(remaining)
		}

		chunk := make([]byte, currentChunkSize)
		if _, err := rand.Read(chunk); err != nil {
			return err
		}

		if _, err := file.Write(chunk); err != nil {
			return err
		}

		written += int64(currentChunkSize)
	}

	return nil
}

func benchmarkUpload(filename string) (*BenchmarkResult, error) {
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	start := time.Now()

	// Run the upload command
	cmd := exec.Command("./file_client/file_client", "upload-file", filename)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("upload command failed: %v", err)
	}

	duration := time.Since(start)
	runtime.ReadMemStats(&memAfter)

	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	throughput := float64(fileInfo.Size()) / (1024 * 1024) / duration.Seconds()

	return &BenchmarkResult{
		Operation:   "Upload",
		Duration:    duration,
		FileSize:    fileInfo.Size(),
		Throughput:  throughput,
		MemoryUsage: memAfter,
	}, nil
}

func benchmarkDownload(remotePath, localPath string) (*BenchmarkResult, error) {
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	start := time.Now()

	// Run the download command
	cmd := exec.Command("./file_client/file_client", "download-file", remotePath, localPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("download command failed: %v", err)
	}

	duration := time.Since(start)
	runtime.ReadMemStats(&memAfter)

	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return nil, err
	}

	throughput := float64(fileInfo.Size()) / (1024 * 1024) / duration.Seconds()

	return &BenchmarkResult{
		Operation:   "Download",
		Duration:    duration,
		FileSize:    fileInfo.Size(),
		Throughput:  throughput,
		MemoryUsage: memAfter,
	}, nil
}

func verifyFiles(original, downloaded string) error {
	orig, err := os.Open(original)
	if err != nil {
		return err
	}
	defer orig.Close()

	down, err := os.Open(downloaded)
	if err != nil {
		return err
	}
	defer down.Close()

	const bufSize = 8192
	buf1 := make([]byte, bufSize)
	buf2 := make([]byte, bufSize)

	for {
		n1, err1 := orig.Read(buf1)
		n2, err2 := down.Read(buf2)

		if n1 != n2 {
			return fmt.Errorf("files differ in size")
		}

		if n1 > 0 {
			for i := 0; i < n1; i++ {
				if buf1[i] != buf2[i] {
					return fmt.Errorf("files differ in content")
				}
			}
		}

		if err1 == io.EOF && err2 == io.EOF {
			break
		}

		if err1 != nil || err2 != nil {
			return fmt.Errorf("error reading files: %v, %v", err1, err2)
		}
	}

	return nil
}

func printBenchmarkResult(result *BenchmarkResult) {
	fmt.Printf("=== %s Results ===\n", result.Operation)
	fmt.Printf("   Duration: %.2fs\n", result.Duration.Seconds())
	fmt.Printf("   File Size: %.2f GB\n", float64(result.FileSize)/(1024*1024*1024))
	fmt.Printf("   Throughput: %.2f MB/s\n", result.Throughput)
	fmt.Printf("   Memory Usage:\n")
	fmt.Printf("     Allocated: %.2f MB\n", float64(result.MemoryUsage.Alloc)/(1024*1024))
	fmt.Printf("     Total Allocated: %.2f MB\n", float64(result.MemoryUsage.TotalAlloc)/(1024*1024))
	fmt.Printf("     System Memory: %.2f MB\n", float64(result.MemoryUsage.Sys)/(1024*1024))
	fmt.Printf("     Heap Objects: %d\n", result.MemoryUsage.HeapObjects)
	fmt.Printf("     GC Cycles: %d\n", result.MemoryUsage.NumGC)
}
