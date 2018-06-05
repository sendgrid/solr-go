package solr_test

import (
	"errors"
	"github.com/sendgrid/go-solr"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Adaptive Router", func() {
	Describe("Test Routing on errors", func() {
		It("routes to best node", func() {
			r := solr.NewAdaptiveRouter(1)
			r.AddSearchResult(time.Second, "http://err.foo.bar", http.StatusOK, errors.New("err"))
			r.AddSearchResult(time.Second, "http://internal-err.foo.bar", http.StatusInternalServerError, errors.New("err"))
			r.AddSearchResult(time.Second, "http://foo.bar", http.StatusOK, nil)
			uri := r.GetUriFromList([]string{"http://err.foo.bar", "http://internal-err.foo.bar", "http://foo.bar"})
			Expect(uri).To(BeEquivalentTo("http://foo.bar"))
		})
	})

	Describe("Test Routing on latency", func() {
		It("routes to fastest node", func() {
			r := solr.NewAdaptiveRouter(1)
			r.AddSearchResult(time.Second, "http://s.foo.bar", http.StatusOK, nil)
			r.AddSearchResult(time.Millisecond, "http://ms.foo.bar", http.StatusOK, nil)
			r.AddSearchResult(time.Minute, "http://m.foo.bar", http.StatusOK, nil)
			uri := r.GetUriFromList([]string{"http://s.foo.bar", "http://m.foo.bar", "http://ms.foo.bar"})
			Expect(uri).To(BeEquivalentTo("http://ms.foo.bar"))
		})
	})
})
