package solr

import (
	"github.com/spaolacci/murmur3"
	"strconv"
	"strings"
)

func Hash(key CompositeKey) int32 {
	mask := 32 - key.Bits
	mask0 := int32(-1 << mask)
	mask1 := int32(-1 ^ mask0)

	hashes := make([]int32, 2)
	hashes[0] = int32(murmur3.Sum32([]byte(key.ShardKey)))
	if key.DocID != "" {
		hashes[1] = int32(murmur3.Sum32([]byte(key.DocID)))
	} else {
		hashes[1] = int32(0)
	}
	return (hashes[0] & mask0) | (hashes[1] & mask1)
}

func ConvertToHashRange(hashRange string) (HashRange, error) {
	ranges := strings.Split(hashRange, "-")
	var rangeReturn HashRange
	if len(ranges) == 2 {
		low, err := strconv.ParseInt(ranges[0], 16, 64)
		if err != nil {
			return rangeReturn, err
		}
		rangeReturn.Low = int32(low)
		high, err := strconv.ParseInt(ranges[1], 16, 64)
		if err != nil {
			return rangeReturn, err
		}
		rangeReturn.High = int32(high)
	}
	return rangeReturn, nil
}
