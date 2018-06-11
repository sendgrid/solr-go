package solr

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Hash Functions", func() {
	Describe("Hash with Doc ID", func() {
		It("is created", func() {
			expected := int32(-1530629653)
			c := CompositeKey{
				ShardKey: "foobar",
				DocID:    "123",
				Bits:     16,
			}
			hash := Hash(c)
			Expect(hash).To(BeEquivalentTo(expected))
		})
	})
	Describe("Hash without Doc ID", func() {
		It("is created", func() {
			expected := int32(-1530658816)
			c := CompositeKey{
				ShardKey: "foobar",
				Bits:     16,
			}
			hash := Hash(c)
			Expect(hash).To(BeEquivalentTo(expected))
		})
	})
})
