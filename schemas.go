package solr

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type SolrZK interface {
	GetZookeepers() string
	GetClusterState() (ClusterState, error)
	GetClusterProps() (ClusterProps, error)
	Listen() error
	Listening() bool
	GetSolrLocator() SolrLocator
	UseHTTPS() (bool, error)
}

type SolrLocator interface {
	GetLeaders(docID string) ([]string, error)
	GetReplicaUris() ([]string, error)
	GetReplicasFromRoute(route string) ([]string, error)
	GetShardFromRoute(route string) (string, error)
	GetLeadersAndReplicas(docID string) ([]string, error)
}

type SolrHTTP interface {
	Select(nodeUris []string, opts ...func(url.Values)) (SolrResponse, error)
	Update(nodeUris []string, singleDoc bool, doc interface{}, opts ...func(url.Values)) error
	Logger() Logger
}

type Logger interface {
	Error(err error)
	Info(v ...interface{})
	Debug(v ...interface{})
	Printf(format string, v ...interface{})
}

type SolrLogger struct {
	*log.Logger
}

func (l *SolrLogger) Error(err error) {
	log.Println(err)
}
func (l *SolrLogger) Info(v ...interface{}) {
	log.Println(v...)
}
func (l *SolrLogger) Debug(v ...interface{}) {
	log.Println(v...)
}
func (l *SolrLogger) Printf(format string, v ...interface{}) {
	l.Debug(fmt.Sprintf(format, v...))
}

type HTTPer interface {
	Do(*http.Request) (*http.Response, error)
}

type CompositeKey struct {
	ShardKey string
	DocID    string
	Bits     uint
}

type HashRange struct {
	Low  int32
	High int32
}

func NewCompositeKey(id string) (CompositeKey, error) {

	if !strings.Contains(id, "!") {
		return CompositeKey{DocID: id}, nil
	}
	keys := strings.Split(id, "!")
	if len(keys) == 1 {
		return CompositeKey{ShardKey: id}, nil
	} else if len(keys) == 2 {
		shard := keys[0]
		bitShift := 0
		var err error
		i := strings.Index(shard, "/")
		if i > 0 {
			bits := strings.Split(shard, "/")
			shard = bits[0]
			bitShift, err = strconv.Atoi(bits[1])
			if err != nil {
				return CompositeKey{}, err
			}
		}
		if bitShift > 16 {
			return CompositeKey{}, errors.New(shard + " contains a bit greater than 16")
		}
		return CompositeKey{ShardKey: keys[0], DocID: shard, Bits: uint(bitShift)}, nil
	}
	return CompositeKey{}, fmt.Errorf("Cant deal with composite keys %s", id)
}

type ClusterProps struct {
	UrlScheme string `json:"urlScheme"`
}
