![SendGrid Logo](https://uiux.s3.amazonaws.com/2016-logos/email-logo%402x.png)

[![travis](https://travis-ci.org/sendgrid/go-solr.svg?branch=master)](https://travis-ci.org/sendgrid/go-solr)
[![Go Report Card](https://goreportcard.com/badge/github.com/sendgrid/go-solr)](https://goreportcard.com/report/github.com/sendgrid/go-solr)
[![GoDoc](https://godoc.org/github.com/sendgrid/go-solr?status.svg)](https://godoc.org/github.com/sendgrid/go-solr)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![CLA assistant](https://cla.sendgrid.com/readme/badge/sendgrid/go-solr)](https://cla.sendgrid.com/sendgrid/go-solr)
[![GitHub contributors](https://img.shields.io/github/contributors/sendgrid/docs.svg)](https://github.com/sendgrid/go-solr/graphs/contributors)

# go-solr
solr go client from Sendgrid

## Usage
To start the client
```
solrzk := solr.NewSolrZK(...)
solrzk.Listen()
solrhttp := solr.NewSolrHttp(solrzk)
solrClient := solr.NewSolrHttpRetrier(solrhttp)
```
The Read and Update methods take a node list use the SolrLocator interface to return a node list

```
locator := solr.GetSolrLocator(solr.NewSolrZK(...))
type SolrLocator interface {
	GetLeaders(docID string) ([]string, error)
	GetReplicaUris() ([]string, error)
	GetReplicasFromRoute(route string) ([]string, error)
	GetLeadersAndReplicas(docID string) ([]string, error)
}
```


To make requests
```
solrClient.Select(locator.GetReplicasFromRoute("shard!"),solr.FilterQuery("myfield:test"),solr.Route("shardkey!"))
```
To make updates
```
solrClient.Update(locator.GetLeadersAndReplicas("{anydocidtoroute}"),collectionName,callsSolrJsonDocs, docsMap)
```

## Tests on solr
1. ```docker-compose up ```
2. ```docker-compose run gotests bash ```
3. ```go test ```
4. ```go run ./cmd/solrRunner.go 1000 ```

## Tests with cluster of 3 solrs
1. ```docker-compose -p cluster -f docker-compose.cluster.yml up ```
2. ```docker-compose -p cluster run gotests bash ```
3. ```go test ```
4. ```go run ./cmd/solrRunner.go 1000 ```



## License
[The MIT License (MIT)](LICENSE)
