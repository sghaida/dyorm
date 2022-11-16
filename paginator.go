package dynamodb

// IdxRange defines lower an upper bound of slice
type IdxRange struct {
	Low, High int
}

// Partition slices a slice and returns lower an upper bound of each sub slice
func Partition(collectionLen, partitionSize int) chan IdxRange {
	c := make(chan IdxRange)
	if partitionSize <= 0 {
		close(c)
		return c
	}

	go func() {
		numFullPartitions := collectionLen / partitionSize
		i := 0
		for ; i < numFullPartitions; i++ {
			c <- IdxRange{Low: i * partitionSize, High: (i + 1) * partitionSize}
		}

		if collectionLen%partitionSize != 0 { // left over
			c <- IdxRange{Low: i * partitionSize, High: collectionLen}
		}
		close(c)
	}()
	return c
}
