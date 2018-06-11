package solr_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sendgrid/go-solr"
	"net/http"
	"time"
)

var _ = Describe("Round Robin Router", func() {
	Describe("Test Routing", func() {
		It("routes to next node", func() {
			r := solr.NewRoundRobinRouter()
			r.AddSearchResult(time.Second, "http://a.foo.bar", http.StatusOK, nil)
			r.AddSearchResult(time.Second, "http://b.foo.bar", http.StatusOK, nil)
			r.AddSearchResult(time.Second, "http://c.foo.bar", http.StatusOK, nil)
			uriA1 := r.GetUriFromList([]string{"http://a.foo.bar", "http://b.foo.bar", "http://c.foo.bar"})
			Expect(uriA1).To(BeEquivalentTo("http://a.foo.bar"))
			uriB1 := r.GetUriFromList([]string{"http://a.foo.bar", "http://b.foo.bar", "http://c.foo.bar"})
			Expect(uriB1).To(BeEquivalentTo("http://b.foo.bar"))
			uriC1 := r.GetUriFromList([]string{"http://a.foo.bar", "http://b.foo.bar", "http://c.foo.bar"})
			Expect(uriC1).To(BeEquivalentTo("http://c.foo.bar"))
			uriA2 := r.GetUriFromList([]string{"http://a.foo.bar", "http://b.foo.bar", "http://c.foo.bar"})
			Expect(uriA2).To(BeEquivalentTo("http://a.foo.bar"))
		})
	})
})
