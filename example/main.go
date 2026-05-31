package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hekimapro/snowflake"
)

func main() {
	fmt.Println("=== Snowflake ID Generator Examples ===")

	// Example 1: Basic usage
	exampleBasicUsage()

	// Example 2: Multiple workers
	exampleMultipleWorkers()

	// Example 3: Concurrent generation
	exampleConcurrentGeneration()

	// Example 4: Extracting ID components
	exampleExtractComponents()

	// Example 5: Custom epoch
	exampleCustomEpoch()
}

func exampleBasicUsage() {
	fmt.Println("1. Basic Usage")
	fmt.Println("   -----------")

	// Create a generator for worker node 1
	generator, err := snowflake.NewGenerator(snowflake.Configuration{
		WorkerIdentifier: 1,
	})
	if err != nil {
		log.Fatalf("Failed to create generator: %v", err)
	}

	// Generate 5 IDs
	for i := 0; i < 5; i++ {
		id := generator.NextID()
		fmt.Printf("   Generated ID %d: %d\n", i+1, id)
	}
	fmt.Println()
}

func exampleMultipleWorkers() {
	fmt.Println("2. Multiple Worker Nodes")
	fmt.Println("   --------------------")

	// Create three different worker nodes
	workers := make([]*snowflake.Generator, 3)
	for i := 0; i < 3; i++ {
		worker, err := snowflake.NewGenerator(snowflake.Configuration{
			WorkerIdentifier: int64(i + 1),
		})
		if err != nil {
			log.Fatalf("Failed to create worker %d: %v", i+1, err)
		}
		workers[i] = worker
	}

	// Generate one ID from each worker
	for idx, worker := range workers {
		id := worker.NextID()
		workerID := snowflake.ExtractWorkerIdentifier(id)
		fmt.Printf("   Worker %d generated ID: %d (worker ID: %d)\n", idx+1, id, workerID)
	}
	fmt.Println()
}

func exampleConcurrentGeneration() {
	fmt.Println("3. Concurrent Generation (100 goroutines)")
	fmt.Println("   --------------------------------------")

	generator, err := snowflake.NewGenerator(snowflake.Configuration{
		WorkerIdentifier: 1,
	})
	if err != nil {
		log.Fatalf("Failed to create generator: %v", err)
	}

	const goroutineCount = 100
	const idsPerGoroutine = 1000

	var waitGroup sync.WaitGroup
	idChannel := make(chan int64, goroutineCount*idsPerGoroutine)
	startTime := time.Now()

	// Launch goroutines
	for i := 0; i < goroutineCount; i++ {
		waitGroup.Add(1)
		go func(workerID int) {
			defer waitGroup.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				idChannel <- generator.NextID()
			}
		}(i)
	}

	// Wait for all goroutines to complete
	waitGroup.Wait()
	close(idChannel)

	duration := time.Since(startTime)

	// Count unique IDs
	uniqueIDs := make(map[int64]bool)
	for id := range idChannel {
		uniqueIDs[id] = true
	}

	totalIDs := goroutineCount * idsPerGoroutine
	fmt.Printf("   Generated %d IDs in %v\n", totalIDs, duration)
	fmt.Printf("   Unique IDs: %d\n", len(uniqueIDs))
	fmt.Printf("   Duplicates: %d\n", totalIDs-len(uniqueIDs))
	fmt.Printf("   Rate: %.0f IDs/second\n", float64(totalIDs)/duration.Seconds())
	fmt.Println()
}

func exampleExtractComponents() {
	fmt.Println("4. Extracting ID Components")
	fmt.Println("   -----------------------")

	generator, err := snowflake.NewGenerator(snowflake.Configuration{
		WorkerIdentifier: 42,
		CustomEpoch:      1735689600000, // 2025-01-01
	})
	if err != nil {
		log.Fatalf("Failed to create generator: %v", err)
	}

	// Generate an ID
	id := generator.NextID()

	// Extract all components
	workerID := snowflake.ExtractWorkerIdentifier(id)
	sequence := snowflake.ExtractSequence(id)
	timestamp := snowflake.ExtractTimestamp(id, 1735689600000)

	// Convert timestamp to human-readable time
	generationTime := time.UnixMilli(timestamp)

	fmt.Printf("   ID: %d\n", id)
	fmt.Printf("   Worker ID: %d\n", workerID)
	fmt.Printf("   Sequence: %d\n", sequence)
	fmt.Printf("   Timestamp: %d (%s)\n", timestamp, generationTime.Format(time.RFC3339))
	fmt.Println()
}

func exampleCustomEpoch() {
	fmt.Println("5. Custom Epoch (Company Launch Date)")
	fmt.Println("   ---------------------------------")

	// Set epoch to your company's launch date
	// Example: June 1, 2020 00:00:00 UTC
	companyLaunchDate := int64(1590969600000)

	generator, err := snowflake.NewGenerator(snowflake.Configuration{
		WorkerIdentifier: 1,
		CustomEpoch:      companyLaunchDate,
	})
	if err != nil {
		log.Fatalf("Failed to create generator: %v", err)
	}

	id := generator.NextID()
	extractedTimestamp := snowflake.ExtractTimestamp(id, companyLaunchDate)
	generationTime := time.UnixMilli(extractedTimestamp)

	fmt.Printf("   Company launch epoch: %s\n", time.UnixMilli(companyLaunchDate).Format(time.RFC3339))
	fmt.Printf("   Generated ID: %d\n", id)
	fmt.Printf("   ID generation time: %s\n", generationTime.Format(time.RFC3339Nano))
	fmt.Println()
}
