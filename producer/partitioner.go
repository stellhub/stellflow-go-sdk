package producer

import (
	"errors"
	"hash/fnv"
)

// KeyHashPartitioner maps records with keys to stable partitions.
func KeyHashPartitioner(record Record, partitions []int32) (int32, error) {
	if len(partitions) == 0 {
		return 0, errors.New("partitions must not be empty")
	}
	if len(record.Key) == 0 {
		return partitions[0], nil
	}
	hash := fnv.New32a()
	_, _ = hash.Write(record.Key)
	return partitions[int(hash.Sum32()%uint32(len(partitions)))], nil
}
