package solr

import (
	"net/http"
	"sort"
	"sync"
	"time"
)

type adaptive []*searchHistory

func (s adaptive) Len() int {
	return len(s)
}

func (s adaptive) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s adaptive) Less(i, j int) bool {
	return s[i].getErrors() < s[j].getErrors() && s[i].getAvgLatency() < s[j].getAvgLatency()
}

type adaptiveRouter struct {
	history map[string]*searchHistory
	recency int
	lock    *sync.RWMutex
}

func (q *adaptiveRouter) GetUriFromList(urisIn []string) string {
	q.lock.RLock()
	defer q.lock.RUnlock()
	searchHistory := make(adaptive, len(urisIn))
	for i, uri := range urisIn {
		if v, ok := q.history[uri]; ok {
			searchHistory[i] = v
		} else {
			searchHistory[i] = newLatencyHistory(uri, q.recency)
		}
	}

	sort.Sort(searchHistory)
	sorted := make([]string, len(searchHistory))
	for i, uri := range searchHistory {
		sorted[i] = uri.uri
	}
	return sorted[0]
}

func (q *adaptiveRouter) AddSearchResult(t time.Duration, uri string, resp *http.Response, err error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if _, ok := q.history[uri]; !ok {
		q.history[uri] = newLatencyHistory(uri, q.recency)
	}
	success := (err != nil && resp.StatusCode == http.StatusOK)
	q.history[uri].addSearchResult(t, !success)
}

type searchHistory struct {
	timings []time.Duration
	errors  []bool
	uri     string
	offset  int
}

func newLatencyHistory(uri string, recency int) *searchHistory {
	return &searchHistory{uri: uri, timings: make([]time.Duration, recency), errors: make([]bool, recency)}
}

func (u *searchHistory) addSearchResult(timing time.Duration, error bool) {
	u.timings[u.offset] = timing
	u.errors[u.offset] = error
	u.offset++
	if u.offset == len(u.timings) {
		u.offset = 0
	}
}

func (u *searchHistory) getErrors() int {
	errors := 0
	for i := 0; i < len(u.errors); i++ {
		if u.errors[i] {
			errors++
		}
	}
	return errors
}

func (u *searchHistory) getAvgLatency() time.Duration {
	total := time.Duration(0)
	for i := 0; i < len(u.timings); i++ {
		total += u.timings[i]
	}
	return total / time.Duration(len(u.timings))
}

func NewAdaptiveRouter(recency int) Router {
	return &adaptiveRouter{history: make(map[string]*searchHistory), recency: recency, lock: &sync.RWMutex{}}
}
