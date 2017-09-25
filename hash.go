package solr

import (
	"github.com/spaolacci/murmur3"
	"strconv"
	"strings"
)

func Hash(key CompositeKey) int32 {
	mask := 32 - 16 - key.Bits
	if key.DocID != "" {
		hashes := []int32{
			int32(murmur3.Sum32([]byte(key.ShardKey))),
			int32(murmur3.Sum32([]byte(key.DocID))),
		}
		masks := []int32{
			-1 << mask, // -10000000000000000
			65535,      // 1111111111111111
		}
		return (hashes[0] & masks[0]) | (hashes[1] & masks[1])
	}
	hashes := []int32{
		int32(murmur3.Sum32([]byte(key.ShardKey))),
		int32(0),
	}

	masks := []int32{
		-1 << mask, // -10000000000000000
		65535,      // 1111111111111111
	}
	return (hashes[0] & masks[0]) | (hashes[1] & masks[1])
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
