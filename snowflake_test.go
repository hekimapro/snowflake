package snowflake

import (
    "sync"
    "testing"
    "time"
)

func TestNewGenerator(t *testing.T) {
    tests := []struct {
        name        string
        config      Configuration
        shouldError bool
    }{
        {
            name: "valid worker identifier",
            config: Configuration{
                WorkerIdentifier: 1,
            },
            shouldError: false,
        },
        {
            name: "invalid worker identifier negative",
            config: Configuration{
                WorkerIdentifier: -1,
            },
            shouldError: true,
        },
        {
            name: "invalid worker identifier too high",
            config: Configuration{
                WorkerIdentifier: 1024,
            },
            shouldError: true,
        },
        {
            name: "valid worker with custom epoch",
            config: Configuration{
                WorkerIdentifier: 5,
                CustomEpoch:      1609459200000,
            },
            shouldError: false,
        },
    }

    for _, testCase := range tests {
        t.Run(testCase.name, func(t *testing.T) {
            generator, err := NewGenerator(testCase.config)

            if testCase.shouldError && err == nil {
                t.Errorf("Expected error but got none")
            }

            if !testCase.shouldError && err != nil {
                t.Errorf("Expected no error but got: %v", err)
            }

            if !testCase.shouldError && generator == nil {
                t.Errorf("Expected generator instance but got nil")
            }
        })
    }
}

func TestNextID(t *testing.T) {
    generator, err := NewGenerator(Configuration{
        WorkerIdentifier: 1,
    })
    if err != nil {
        t.Fatalf("Failed to create generator: %v", err)
    }

    // Generate first ID
    firstID := generator.NextID()
    if firstID == 0 {
        t.Errorf("Expected non-zero ID, got 0")
    }

    // Generate second ID
    secondID := generator.NextID()
    if secondID == 0 {
        t.Errorf("Expected non-zero ID, got 0")
    }

    // IDs should be unique and increasing
    if secondID <= firstID {
        t.Errorf("Expected second ID > first ID, got %d <= %d", secondID, firstID)
    }
}

func TestNextIDUniqueness(t *testing.T) {
    generator, err := NewGenerator(Configuration{
        WorkerIdentifier: 1,
    })
    if err != nil {
        t.Fatalf("Failed to create generator: %v", err)
    }

    // Generate a large batch of IDs
    const idCount = 100000
    generatedIDs := make(map[int64]bool)

    for i := 0; i < idCount; i++ {
        currentID := generator.NextID()
        if generatedIDs[currentID] {
            t.Errorf("Duplicate ID found: %d", currentID)
        }
        generatedIDs[currentID] = true
    }

    if len(generatedIDs) != idCount {
        t.Errorf("Expected %d unique IDs, got %d", idCount, len(generatedIDs))
    }
}

func TestNextIDConcurrency(t *testing.T) {
    generator, err := NewGenerator(Configuration{
        WorkerIdentifier: 1,
    })
    if err != nil {
        t.Fatalf("Failed to create generator: %v", err)
    }

    const goroutineCount = 100
    const idsPerGoroutine = 1000

    var waitGroup sync.WaitGroup
    idChannel := make(chan int64, goroutineCount*idsPerGoroutine)

    for i := 0; i < goroutineCount; i++ {
        waitGroup.Add(1)
        go func() {
            defer waitGroup.Done()
            for j := 0; j < idsPerGoroutine; j++ {
                idChannel <- generator.NextID()
            }
        }()
    }

    waitGroup.Wait()
    close(idChannel)

    // Check for duplicates
    seenIDs := make(map[int64]bool)
    for currentID := range idChannel {
        if seenIDs[currentID] {
            t.Errorf("Duplicate ID found in concurrent generation: %d", currentID)
        }
        seenIDs[currentID] = true
    }

    expectedTotal := goroutineCount * idsPerGoroutine
    if len(seenIDs) != expectedTotal {
        t.Errorf("Expected %d unique IDs, got %d", expectedTotal, len(seenIDs))
    }
}

func TestNextIDMonotonicity(t *testing.T) {
    generator, err := NewGenerator(Configuration{
        WorkerIdentifier: 1,
    })
    if err != nil {
        t.Fatalf("Failed to create generator: %v", err)
    }

    previousID := generator.NextID()

    for i := 0; i < 10000; i++ {
        currentID := generator.NextID()
        if currentID <= previousID {
            t.Errorf("IDs not monotonic: %d <= %d at iteration %d", currentID, previousID, i)
        }
        previousID = currentID
    }
}

func TestExtractTimestamp(t *testing.T) {
    testEpoch := int64(1735689600000)
    generator, err := NewGenerator(Configuration{
        WorkerIdentifier: 1,
        CustomEpoch:      testEpoch,
    })
    if err != nil {
        t.Fatalf("Failed to create generator: %v", err)
    }

    beforeGeneration := time.Now().UnixMilli()
    generatedID := generator.NextID()
    afterGeneration := time.Now().UnixMilli()

    extractedTimestamp := ExtractTimestamp(generatedID, testEpoch)

    if extractedTimestamp < beforeGeneration {
        t.Errorf("Extracted timestamp %d is before generation start %d", extractedTimestamp, beforeGeneration)
    }

    if extractedTimestamp > afterGeneration {
        t.Errorf("Extracted timestamp %d is after generation end %d", extractedTimestamp, afterGeneration)
    }
}

func TestExtractWorkerIdentifier(t *testing.T) {
    expectedWorkerID := int64(42)
    generator, err := NewGenerator(Configuration{
        WorkerIdentifier: expectedWorkerID,
    })
    if err != nil {
        t.Fatalf("Failed to create generator: %v", err)
    }

    generatedID := generator.NextID()
    extractedWorkerID := ExtractWorkerIdentifier(generatedID)

    if extractedWorkerID != expectedWorkerID {
        t.Errorf("Expected worker ID %d, got %d", expectedWorkerID, extractedWorkerID)
    }
}

func TestExtractSequence(t *testing.T) {
    generator, err := NewGenerator(Configuration{
        WorkerIdentifier: 1,
    })
    if err != nil {
        t.Fatalf("Failed to create generator: %v", err)
    }

    // Generate multiple IDs in quick succession to get sequence numbers
    firstID := generator.NextID()
    secondID := generator.NextID()

    firstSequence := ExtractSequence(firstID)
    secondSequence := ExtractSequence(secondID)

    // Sequence should increase or reset
    if secondSequence < firstSequence && secondSequence != 0 {
        t.Logf("Sequence may have wrapped: first=%d, second=%d", firstSequence, secondSequence)
    }
}

func TestDifferentWorkers(t *testing.T) {
    worker1, err := NewGenerator(Configuration{WorkerIdentifier: 1})
    if err != nil {
        t.Fatalf("Failed to create worker 1: %v", err)
    }

    worker2, err := NewGenerator(Configuration{WorkerIdentifier: 2})
    if err != nil {
        t.Fatalf("Failed to create worker 2: %v", err)
    }

    // Generate IDs from both workers
    worker1ID := worker1.NextID()
    worker2ID := worker2.NextID()

    // Extract worker identifiers
    worker1Extracted := ExtractWorkerIdentifier(worker1ID)
    worker2Extracted := ExtractWorkerIdentifier(worker2ID)

    if worker1Extracted != 1 {
        t.Errorf("Expected worker 1 extracted ID to be 1, got %d", worker1Extracted)
    }

    if worker2Extracted != 2 {
        t.Errorf("Expected worker 2 extracted ID to be 2, got %d", worker2Extracted)
    }
}

func TestPerformance(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping performance test in short mode")
    }

    generator, err := NewGenerator(Configuration{
        WorkerIdentifier: 1,
    })
    if err != nil {
        t.Fatalf("Failed to create generator: %v", err)
    }

    const iterations = 1000000
    startTime := time.Now()

    for i := 0; i < iterations; i++ {
        generator.NextID()
    }

    duration := time.Since(startTime)
    averageNs := duration.Nanoseconds() / iterations

    // Should be under 500 nanoseconds per ID
    if averageNs > 500 {
        t.Errorf("Performance too slow: %d ns/op, expected <500", averageNs)
    }

    t.Logf("Average ID generation time: %d ns/op", averageNs)
}