package solr

import (
	"sync"
	"time"
)

type Router interface {
	GetUriFromList(urisIn []string) string
	AddSearchResult(t time.Duration, uri string, statusCode int, err error)
}

type roundRobinRouter struct {
	lastQuery map[string]time.Time
	lock      *sync.RWMutex
}

func (r *roundRobinRouter) GetUriFromList(urisIn []string) string {
	r.lock.Lock()
	defer r.lock.Unlock()
	var oldestValue time.Time
	var result string
	for _, uri := range urisIn {
		if v, ok := r.lastQuery[uri]; !ok {
			result = uri
			break
		} else if (oldestValue == time.Time{}) || v.Before(oldestValue) {
			oldestValue = v
			result = uri
		}
	}
	r.lastQuery[result] = time.Now()
	return result
}

func (r *roundRobinRouter) AddSearchResult(t time.Duration, uri string, statusCode int, err error) {
	//no-op, updated on GetUriFromList
}

func NewRoundRobinRouter() Router {
	return &roundRobinRouter{lock: &sync.RWMutex{}, lastQuery: make(map[string]time.Time)}
}
