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
	if s[i].getErrors() < s[j].getErrors() {
		return true
	} else if s[i].getErrors() == s[j].getErrors() && s[i].getMedianLatency() < s[j].getMedianLatency() {
		return true
	}
	return false
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
			searchHistory[i] = newSearchHistory(uri, q.recency)
		}
	}

	sort.Sort(searchHistory)
	return searchHistory[0].uri
}

func (q *adaptiveRouter) AddSearchResult(t time.Duration, uri string, statusCode int, err error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if _, ok := q.history[uri]; !ok {
		q.history[uri] = newSearchHistory(uri, q.recency)
	}
	success := (err == nil && statusCode >= http.StatusOK && statusCode < http.StatusBadRequest)
	q.history[uri].addSearchResult(t, !success)
}

type searchHistory struct {
	timings []time.Duration
	errors  []bool
	uri     string
	offset  int
	lock    *sync.RWMutex
}

func newSearchHistory(uri string, recency int) *searchHistory {
	return &searchHistory{
		uri:     uri,
		timings: make([]time.Duration, recency),
		errors:  make([]bool, recency),
		lock:    &sync.RWMutex{},
	}
}

func (u *searchHistory) addSearchResult(timing time.Duration, error bool) {
	u.lock.Lock()
	defer u.lock.Unlock()
	u.timings[u.offset] = timing
	u.errors[u.offset] = error
	u.offset++
	if u.offset == len(u.timings) {
		u.offset = 0
	}
}

func (u *searchHistory) getErrors() int {
	u.lock.RLock()
	defer u.lock.RUnlock()
	var errors int
	for i := 0; i < len(u.errors); i++ {
		if u.errors[i] {
			errors++
		}
	}
	return errors
}

func (u *searchHistory) getMedianLatency() time.Duration {
	u.lock.RLock()
	defer u.lock.RUnlock()
	tmp := make([]time.Duration, len(u.timings))
	copy(tmp, u.timings)
	sort.Slice(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
	return tmp[len(u.timings)/2]
}

func NewAdaptiveRouter(recency int) Router {
	return &adaptiveRouter{
		history: make(map[string]*searchHistory),
		recency: recency,
		lock:    &sync.RWMutex{},
	}
}
