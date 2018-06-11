package solr_test

import (
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sendgrid/go-solr"
)

func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

var _ = Describe("Solr Client", func() {
	var solrClient solr.SolrZK
	var solrHttp solr.SolrHTTP
	var solrHttpRetrier solr.SolrHTTP
	var locator solr.SolrLocator
	solrClient = solr.NewSolrZK("zk:2181", "solr", "solrtest")
	locator = solrClient.GetSolrLocator()

	err := solrClient.Listen()
	BeforeEach(func() {
		Expect(err).To(BeNil())
		https, _ := solrClient.UseHTTPS()
		solrHttp, err = solr.NewSolrHTTP(https, "solrtest", solr.User("solr"), solr.Password("admin"), solr.MinRF(2))
		Expect(err).To(BeNil())
		solrHttpRetrier = solr.NewSolrHttpRetrier(solrHttp, 5, 100*time.Millisecond)
	})
	It("construct", func() {
		solrClient := solr.NewSolrZK("test", "solr", "solrtest")
		Expect(solrClient).To(Not(BeNil()))
		err := solrClient.Listen()
		Expect(err).To(Not(BeNil()))

	})

	Describe("Test Connection", func() {

		It("can get clusterstate", func() {
			state, err := solrClient.GetClusterState()
			Expect(err).To(BeNil())
			Expect(state).To(Not(BeNil()))
			Expect(state.Version > 0).To(BeTrue())
			Expect(len(state.Collections)).To(Equal(1))
		})

		It("can find a leader", func() {
			state, err := solrClient.GetClusterState()
			Expect(err).To(BeNil())
			Expect(state).To(Not(BeNil()))
			Expect(len(state.Collections)).To(Equal(1))
			leaders, err := locator.GetLeaders("mycrazyshardkey1!test.1@test.com")
			Expect(err).To(BeNil())
			leader := leaders[0]
			Expect(leader).To(Not(BeNil()))
			Expect(leader).To(ContainSubstring(":8983/solr"))
			Expect(leader).To(ContainSubstring("http://"))
		})

		It("can find a replica", func() {
			state, err := solrClient.GetClusterState()
			Expect(err).To(BeNil())
			Expect(state).To(Not(BeNil()))
			Expect(len(state.Collections)).To(Equal(1))
			replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey1!")
			Expect(err).To(BeNil())
			Expect(len(replicas)).To(Not(BeZero()))

			Expect(replicas[0]).To(ContainSubstring(":8983/solr"))
			Expect(replicas[0]).To(ContainSubstring("http://"))
		})

		Describe("Test Requests", func() {
			It("can get requests", func() {
				replicas, err := locator.GetReplicaUris()
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.FilterQuery("*:*"), solr.Rows(10))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
			})
			It("can update requests", func() {
				uuid, _ := newUUID()

				doc := map[string]interface{}{
					"id":         "mycrazyshardkey1!" + uuid,
					"email":      uuid + "feldman1@sendgrid.com",
					"first_name": "shawn1" + uuid,
					"last_name":  uuid + "feldman1",
				}
				leader, err := locator.GetLeaders("mycrazyshardkey1!" + uuid)
				Expect(err).To(BeNil())
				err = solrHttp.Update(leader, true, doc, solr.Commit(true))
				Expect(err).To(BeNil())
				replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey1!")
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.Query("*:*"), solr.FilterQuery("first_name:shawn1"+uuid), solr.Rows(10))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(1))
			})

			It("can update requests with no doc id", func() {
				uuid, _ := newUUID()

				doc := map[string]interface{}{
					"id":         "mycrazyshardkey1!" + uuid,
					"email":      uuid + "feldman1@sendgrid.com",
					"first_name": "shawn1" + uuid,
					"last_name":  uuid + "feldman1",
				}
				leader, err := locator.GetLeaders("mycrazyshardkey1!" + uuid)
				Expect(err).To(BeNil())
				err = solrHttp.Update(leader, true, doc, solr.Commit(true))
				Expect(err).To(BeNil())
				replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey1!")
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.Query("*:*"), solr.FilterQuery("first_name:shawn1"+uuid), solr.Rows(10))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(1))
			})

			It("can update requests with route", func() {
				uuid, _ := newUUID()

				doc := map[string]interface{}{
					"id":         "mycrazyshardkey1!" + uuid,
					"email":      uuid + "feldman1@sendgrid.com",
					"first_name": "shawn1" + uuid,
					"last_name":  uuid + "feldman1",
				}
				leader, err := locator.GetLeaders("mycrazyshardkey1!" + uuid)
				Expect(err).To(BeNil())
				err = solrHttp.Update(leader, true, doc, solr.Commit(true), solr.Route("mycrazyshardkey1!"))
				Expect(err).To(BeNil())
				replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey1!")
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.Query("*:*"), solr.FilterQuery("first_name:shawn1"+uuid), solr.Rows(10))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(1))
			})

			It("test prefer local", func() {
				uuid, _ := newUUID()

				doc := map[string]interface{}{
					"id":         "mycrazyshardkey1!" + uuid,
					"email":      uuid + "feldman1@sendgrid.com",
					"first_name": "shawn1" + uuid,
					"last_name":  uuid + "feldman1",
				}
				leader, err := locator.GetLeaders("mycrazyshardkey1!" + uuid)
				Expect(err).To(BeNil())
				err = solrHttp.Update(leader, true, doc, solr.Commit(true), solr.Route("mycrazyshardkey1!"))
				Expect(err).To(BeNil())
				replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey1!")
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.Query("*:*"), solr.FilterQuery("first_name:shawn1"+uuid), solr.Rows(10), solr.PreferLocalShards(true))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(1))
			})

			It("can update requests with route with version", func() {
				uuid, _ := newUUID()

				doc := map[string]interface{}{
					"id":         "mycrazyshardkey1!" + uuid,
					"email":      uuid + "feldman1@sendgrid.com",
					"first_name": "shawn1" + uuid,
					"last_name":  uuid + "feldman1",
				}
				state, err := solrClient.GetClusterState()
				Expect(err).To(BeNil())
				leader, err := locator.GetLeaders("mycrazyshardkey1!" + uuid)
				Expect(err).To(BeNil())
				err = solrHttp.Update(leader, true, doc, solr.Commit(true), solr.Route("mycrazyshardkey1!"), solr.ClusterStateVersion(state.Version, "goseg"))
				Expect(err).To(BeNil())
				replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey1!")
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.Query("*:*"), solr.FilterQuery("first_name:shawn1"+uuid), solr.Rows(10), solr.ClusterStateVersion(state.Version, "goseg"))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(1))
			})

			It("can update requests and read with route", func() {
				uuid, _ := newUUID()

				doc := map[string]interface{}{
					"id":         "mycrazyshardkey3!" + uuid,
					"email":      uuid + "feldman@sendgrid.com",
					"first_name": "shawn3" + uuid,
					"last_name":  uuid,
				}
				leader, err := locator.GetLeaders("mycrazyshardkey3!" + uuid)
				Expect(err).To(BeNil())
				err = solrHttp.Update(leader, true, doc, solr.Commit(true))
				Expect(err).To(BeNil())
				replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey3!")
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.Query("*:*"), solr.FilterQuery("last_name:"+uuid), solr.Rows(10))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(1))
			})

			It("can update requests and read with route many times", func() {
				const limit int = 10
				uuid, _ := newUUID()
				for i := 0; i < limit; i++ {
					iterationId, _ := newUUID()
					doc := map[string]interface{}{
						"id":         "mycrazyshardkey4!rando" + iterationId,
						"email":      "rando" + iterationId + "@sendgrid.com",
						"first_name": "tester" + iterationId,
						"last_name":  uuid,
					}
					if i < limit-1 {
						leader, err := locator.GetLeaders(doc["id"].(string))
						Expect(err).To(BeNil())
						err = solrHttp.Update(leader, true, doc, solr.Commit(false))
						Expect(err).To(BeNil())
					} else {
						leader, err := locator.GetLeaders(doc["id"].(string))
						Expect(err).To(BeNil())
						err = solrHttp.Update(leader, true, doc, solr.Commit(true))
						Expect(err).To(BeNil())
					}

				}
				replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey4!")
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.Query("*:*"), solr.FilterQuery("last_name:"+uuid), solr.Rows(1000))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(limit))
			})

			It("can test the retrier requests and read with route many times", func() {
				const limit int = 100
				uuid, _ := newUUID()
				for i := 0; i < limit; i++ {
					iterationId, _ := newUUID()
					doc := map[string]interface{}{
						"id":         "mycrazyshardkey4!rando" + iterationId,
						"email":      "rando" + iterationId + "@sendgrid.com",
						"first_name": "tester" + iterationId,
						"last_name":  uuid,
					}
					if i < limit-1 {
						leader, err := locator.GetLeaders(doc["id"].(string))
						Expect(err).To(BeNil())
						err = solrHttpRetrier.Update(leader, true, doc, solr.Commit(false))
						Expect(err).To(BeNil())
					} else {
						leader, err := locator.GetLeaders(doc["id"].(string))
						Expect(err).To(BeNil())
						err = solrHttpRetrier.Update(leader, true, doc, solr.Commit(true))
						Expect(err).To(BeNil())
					}

				}
				replicas, err := locator.GetReplicasFromRoute("mycrazyshardkey4!")
				Expect(err).To(BeNil())
				r, err := solrHttpRetrier.Select(replicas, solr.Query("*:*"), solr.FilterQuery("last_name:"+uuid), solr.Rows(1000))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(limit))
			})

			It("can delete all", func() {
				lastId := ""
				const limit int = 10
				uuid, _ := newUUID()
				shardKey := "mycrazysha" + uuid
				for i := 0; i < limit; i++ {
					iterationId, _ := newUUID()
					lastId := shardKey + "!rando" + iterationId
					doc := map[string]interface{}{
						"id":         lastId,
						"email":      "rando" + iterationId + "@sendgrid.com",
						"first_name": "tester" + iterationId,
						"last_name":  uuid,
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
				leader, err := locator.GetLeaders(lastId)
				Expect(err).To(BeNil())
				err = solrHttp.Update(leader, false, nil, solr.Commit(true), solr.DeleteStreamBody("last_name:*"))
				Expect(err).To(BeNil())
				replicas, err := locator.GetReplicasFromRoute(shardKey + "!")
				Expect(err).To(BeNil())
				r, err := solrHttp.Select(replicas, solr.Route(shardKey), solr.Query("*:*"), solr.FilterQuery("last_name:"+uuid), solr.Rows(1000))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(0))
			})

			It("can get the shard for a route", func() {
				shard, err := locator.GetShardFromRoute("mycrazyshardkey3!")
				Expect(err).To(BeNil())
				Expect(shard).To(Not(BeNil()))
			})
		})
	})
	Describe("Basic Auth Fails", func() {
		It("can get requests", func() {
			solrNoAuthClient := solr.NewSolrZK("zk:2181", "solr", "solrtest")
			err := solrNoAuthClient.Listen()
			Expect(err).To(BeNil())
			https, _ := solrClient.UseHTTPS()
			solrNoAuthHttp, err := solr.NewSolrHTTP(https, "solrtest")
			Expect(err).To(BeNil())
			err = solrNoAuthClient.Listen()
			Expect(err).To(BeNil())
			replicas, err := locator.GetReplicaUris()
			Expect(err).To(BeNil())
			r, err := solrNoAuthHttp.Select(replicas, solr.FilterQuery("*:*"), solr.Rows(10))
			Expect(err).To(Not(BeNil()))
			Expect(strings.Contains(err.Error(), "401")).To(BeTrue())
			Expect(r.Status).To(BeEquivalentTo(401))
		})

	})

})
