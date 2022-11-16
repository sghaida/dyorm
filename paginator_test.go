package dynamodb_test

import (
	dynamodb "github.com/sghaida/dyorm"
	"math/rand"
	"testing"
)

func TestPartitions(t *testing.T) {
	t.Run("Validate partitions counts", func(t *testing.T) {
		testData1 := make([]int, 100)
		testData2 := make([]int, 100)
		results := make(map[int]int)
		index := 0

		for i := range testData1 {
			testData1[i] = int(rand.Int31n(100))
		}

		for idxRange := range dynamodb.Partition(len(testData1), 10) {
			results[index] = idxRange.Low
			index++
		}

		if len(results) != 10 {
			t.Errorf("Expected size: 10, got %d", len(results))
		}

		results = make(map[int]int)
		index = 0
		for idxRange := range dynamodb.Partition(len(testData2), 15) {
			results[index] = idxRange.Low
			index++
		}

		if len(results) != 7 {
			t.Errorf("Expected size: 7, got %d", len(results))
		}
	})

	t.Run("Validate empty", func(t *testing.T) {
		counter := 0
		res := 0
		for idxRange := range dynamodb.Partition(0, 15) {
			counter++
			res = idxRange.High
		}

		if res != 0 || counter != 0 {
			t.Errorf("Expected size: 0, got %d", res)
		}
	})

	t.Run("partition size < = 0", func(t *testing.T) {
		idxRange := dynamodb.Partition(10, 0)
		res := <-idxRange
		if (res != dynamodb.IdxRange{}) {
			t.Errorf("Expected size: 0, got %d", res)
		}
	})

}
