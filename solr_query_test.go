package solr_test

import (
	"net/url"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sendgrid/go-solr"
)

var _ = Describe("Solr Client", func() {
	var solrClient solr.SolrZK
	var solrHttp solr.SolrHTTP
	var solrHttpRetrier solr.SolrHTTP
	var locator solr.SolrLocator
	solrClient = solr.NewSolrZK("zk:2181", "solr", "solrtest")
	locator = solrClient.GetSolrLocator()
	const limit int = 100
	uuid, _ := newUUID()
	shardKey := "mycrazysha" + uuid

	err := solrClient.Listen()
	BeforeEach(func() {
		Expect(err).To(BeNil())
		https, _ := solrClient.UseHTTPS()
		solrHttp, err = solr.NewSolrHTTP(https, "solrtest", solr.User("solr"), solr.Password("admin"), solr.MinRF(2))
		Expect(err).To(BeNil())
		solrHttpRetrier = solr.NewSolrHttpRetrier(solrHttp, 5, 100*time.Millisecond)

		var salaries = []float32{40000, 60000, 80000, 100000, 120000}
		var firstNames = []string{"john", "jane", "alice", "bob", "ashley", "gordon", "peter", "cindy", "brie", "alex"}
		for i := 0; i < limit; i++ {
			iterationId, _ := newUUID()
			lastId := shardKey + "!rando" + iterationId
			doc := map[string]interface{}{
				"id":         lastId,
				"email":      "rando" + iterationId + "@sendgrid.com",
				"first_name": firstNames[i%len(firstNames)],
				"last_name":  uuid,
				"salary":     salaries[i%len(salaries)],
			}
			leader, err := locator.GetLeaders(doc["id"].(string))
			Expect(err).To(BeNil())

			if i < limit-1 {
				err := solrHttp.Update(leader, true, doc, solr.Commit(false))
				Expect(err).To(BeNil())
			} else {
				err := solrHttp.Update(leader, true, doc, solr.Commit(true))
				Expect(err).To(BeNil())
			}
		}
	})

	AfterEach(func() {
		replicas, err := locator.GetReplicasFromRoute(shardKey + "!")
		Expect(err).To(BeNil())
		err = solrHttp.Update(replicas, false, nil, solr.Commit(true), solr.DeleteStreamBody("last_name:*"))
		Expect(err).To(BeNil())
	})

	Describe("Test Faceting", func() {
		It("can return facets", func() {
			replicas, err := locator.GetReplicaUris()
			Expect(err).To(BeNil())

			query := []func(url.Values){
				solr.Query("*:*"),
				solr.JSONFacet(
					solr.M{
						"max_salary": "max(salary)",
						"salary": solr.M{
							"type":  "terms",
							"field": "salary",
						},
					},
				),
			}

			r, err := solrHttp.Select(replicas, query...)
			Expect(err).To(BeNil())
			Expect(r).To(Not(BeNil()))
			Expect(r.Response.NumFound).To(BeEquivalentTo(limit))
			Expect(r.Facets["max_salary"].(float64)).To(BeEquivalentTo(120000))
			Expect(len(r.Facets["salary"].(map[string]interface{})["buckets"].([]interface{}))).To(BeEquivalentTo(5))
		})
	})
})
