package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	. "github.com/sendgrid/go-solr"
)

var (
	solrHttpRetrier SolrHTTP
	solrZk          SolrZK
	locator         SolrLocator
)

func init() {
	var err error
	solrZk = NewSolrZK("zk:2181", "solr", "solrtest")
	locator = solrZk.GetSolrLocator()
	err = solrZk.Listen()
	if err != nil {
		panic(err)
	}
	https, err := solrZk.UseHTTPS()
	if err != nil {
		panic(err)
	}
	solrHttp, err := NewSolrHTTP(https, "solrtest", User("solr"), Password("admin"), MinRF(2), QueryRouter(NewRoundRobinRouter()))
	if err != nil {
		panic(err)
	}
	solrHttpRetrier = NewSolrHttpRetrier(solrHttp, 5, 100*time.Millisecond)

}
func main() {
	var limit int = 100 * 10
	fmt.Println(os.Args)

	//get first arg number
	if len(os.Args) > 1 {
		var err error
		limit, err = strconv.Atoi(os.Args[1])
		if err != nil {
			panic(fmt.Sprintln("could not convert limit to int"))
		}
	}
	numFound := run(limit, "/3")
	if limit != int(numFound) {
		panic(fmt.Sprintf("limit did not match what was found %d=%d", limit, numFound))
	}
	fmt.Println(fmt.Sprintf("runner done %d", numFound))

	numFound = run(limit, "")
	if limit != int(numFound) {
		panic(fmt.Sprintf("limit did not match what was found %d=%d", limit, numFound))
	}
	fmt.Println(fmt.Sprintf("runner done %d", numFound))
}

func run(limit int, bits string) uint32 {
	shardKeys := []string{}
	for i := 0; i < 10; i++ {
		uuid, _ := newUUID()
		shardKey := fmt.Sprintf("mycrazy%sshardkey%s", uuid, bits)
		shardKeys = append(shardKeys, shardKey)
	}
	uuid, _ := newUUID()
	for i := 0; i < limit; i++ {
		shardKey := shardKeys[i%10]
		iterationId, _ := newUUID()
		doc := map[string]interface{}{
			"id":         fmt.Sprintf("%s!rando%s", shardKey, iterationId),
			"email":      "rando" + iterationId + "@sendgrid.com",
			"first_name": "tester" + iterationId,
			"last_name":  uuid,
		}
		all, err := locator.GetLeadersAndReplicas(doc["id"].(string))
		if err != nil {
			panic(err)
		}
		if i < limit-1 {
			err := solrHttpRetrier.Update(all, true, doc, Commit(false))
			if err != nil {
				panic(err)
			}
		} else {
			err := solrHttpRetrier.Update(all, true, doc, Commit(true))
			if err != nil {
				panic(err)
			}
		}
	}
	//wait for commits
	time.Sleep(time.Second)

	replicas, err := locator.GetReplicaUris()

	if err != nil {
		panic(err)
	}
	r, err := solrHttpRetrier.Select(replicas, Query("*:*"), FilterQuery("last_name:"+uuid), Rows(uint32(limit)))
	if err != nil {
		panic(fmt.Sprintf("error %v getting count on %v", err, replicas))
	}
	sum := 0
	for i := 0; i < 10; i++ {
		shardKey := shardKeys[i%10]

		replicas, err := locator.GetLeadersAndReplicas(shardKey)

		if err != nil {
			panic(err)
		}
		check, err := solrHttpRetrier.Select(replicas, Query("id:"+shardKey+"!rando*"), FilterQuery("last_name:"+uuid), Rows(uint32(0)))
		if err != nil {
			panic(err)
		}
		sum += int(check.Response.NumFound)
		fmt.Println(fmt.Sprintf("sum is %d", sum))
	}
	fmt.Println(fmt.Sprintf("sum is %d", sum))
	return r.Response.NumFound

}
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
