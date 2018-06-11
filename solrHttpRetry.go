package solr

import (
	"errors"
	"fmt"
	"net/url"
	"time"
)

type SolrHttpRetrier struct {
	solrCli            SolrHTTP
	retries            int
	exponentialBackoff time.Duration
	readTimeout        time.Duration
	updateTimeout      time.Duration
}

func NewSolrHttpRetrier(solrHttp SolrHTTP, retries int, exponentialBackoff time.Duration) SolrHTTP {
	solrRetrier := SolrHttpRetrier{solrCli: solrHttp, retries: retries, exponentialBackoff: exponentialBackoff}
	return &solrRetrier
}

func (s *SolrHttpRetrier) Select(nodeUris []string, opts ...func(url.Values)) (SolrResponse, error) {
	if len(nodeUris) == 0 {
		return SolrResponse{}, errors.New("[Solr HTTP Retrier]Length of nodes in solr is empty")
	}
	now := time.Now()
	var resp SolrResponse
	var err error
	backoff := s.exponentialBackoff
	for attempt := 0; attempt < s.retries; attempt++ {
		resp, err = s.solrCli.Select(nodeUris, opts...)
		if err == ErrNotFound {
			return resp, err
		}
		if err != nil {
			s.Logger().Debug(fmt.Sprintf("[Solr Http Retrier] Error Retrying %v ", err))
			backoff = s.backoff(backoff)
			s.Logger().Debug(fmt.Sprintf("Sleeping attempt: %d, for time: %v running for: %v ", attempt, backoff, time.Since(now)))
			continue
		}
		if attempt > 0 {
			s.Logger().Debug(fmt.Sprintf("[Solr Http Retrier] healed after %d", attempt))
		}
		break
	}
	return resp, err
}

func (s *SolrHttpRetrier) Update(nodeUris []string, jsonDocs bool, doc interface{}, opts ...func(url.Values)) error {
	if len(nodeUris) == 0 {
		return errors.New("[Solr HTTP Retrier]Length of nodes in solr is empty")
	}
	now := time.Now()
	var err error
	backoff := s.exponentialBackoff
	for attempt := 0; attempt < s.retries; attempt++ {
		uri := nodeUris[attempt%len(nodeUris)]
		err = s.solrCli.Update([]string{uri}, jsonDocs, doc, opts...)
		if err == ErrNotFound {
			return err
		}
		if err != nil {
			if minRFErr, ok := err.(SolrMinRFError); ok {
				s.Logger().Error(minRFErr)
			} else {
				s.Logger().Debug(fmt.Sprintf("[Solr Http Retrier] Error Retrying %v ", err))
			}
			backoff = s.backoff(backoff)
			s.Logger().Debug(fmt.Sprintf("Sleeping attempt: %d, for time: %v running for: %v ", attempt, backoff, time.Since(now)))
			continue
		}
		if attempt > 0 {
			s.Logger().Debug(fmt.Sprintf("[Solr Http Retrier] Healed after attempt %d", attempt))
		}
		break
	}
	return err
}

func (s *SolrHttpRetrier) Logger() Logger {
	return s.solrCli.Logger()
}

//exponential backoff of the backoffInterval and returns new backoffInterval
func (s *SolrHttpRetrier) backoff(backoffInterval time.Duration) time.Duration {
	backoffInterval = backoffInterval * time.Duration(2)
	time.Sleep(backoffInterval)
	return backoffInterval
}
