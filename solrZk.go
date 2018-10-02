package solr

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
	"sync"
)

type solrZkInstance struct {
	zookeeper         Zookeeper
	collection        string
	host              string
	currentNode       int
	clusterState      ClusterState
	clusterStateMutex *sync.Mutex
	listening         bool
	logger            Logger
	sleepTimeMS       int
}

func NewSolrZK(zookeepers string, zkRoot string, collectionName string, opts ...func(*solrZkInstance)) SolrZK {
	instance := solrZkInstance{
		zookeeper:   NewZookeeper(zookeepers, zkRoot, collectionName),
		sleepTimeMS: 500,
		collection:  collectionName,
	}

	instance.clusterStateMutex = &sync.Mutex{}
	instance.listening = false
	instance.logger = &SolrLogger{log.New(ioutil.Discard, "[SolrClient] ", log.LstdFlags)}
	for _, opt := range opts {
		opt(&instance)
	}

	return &instance
}

func (s *solrZkInstance) GetSolrLocator() SolrLocator {
	return s
}

func SleepTimeMS(sleepTimeMS int) func(*solrZkInstance) {
	return func(s *solrZkInstance) {
		s.sleepTimeMS = sleepTimeMS
	}
}

func (s *solrZkInstance) GetZookeepers() string {
	return s.zookeeper.GetConnectionString()
}

func (s *solrZkInstance) UseHTTPS() (bool, error) {
	var err error
	var props ClusterProps
	props, err = s.GetClusterProps()
	if err != nil {
		return false, err
	}
	return props.UrlScheme == "https", nil
}

func (s *solrZkInstance) GetLeaders(docID string) ([]string, error) {
	cs, err := s.GetClusterState()
	if err != nil {
		return []string{}, err
	}
	collectionMap := cs.Collections[s.collection]
	leader, err := findLeader(docID, &collectionMap)
	return []string{leader}, err
}

func (s *solrZkInstance) GetLeadersAndReplicas(docID string) ([]string, error) {
	var leaderCount int
	leaders, err := s.GetLeaders(docID)
	if err != nil {
		return nil, err
	}
	keys := strings.Split(docID, "!")
	replicas, err := s.GetReplicasFromRoute(keys[0])
	if err != nil {
		return nil, err
	}

	set := make(map[string]bool, len(leaders)+len(replicas))
	all := make([]string, 0, len(replicas))
	for _, v := range leaders {
		set[v] = true
		if v != "" {
			all = append(all, v)
			leaderCount++
		}
	}
	for _, v := range replicas {
		if there := set[v]; !there {
			if v != "" {
				all = append(all, v)
				set[v] = true
			}
		}
	}
	if leaderCount == 0 {
		s.logger.Debug(fmt.Sprintf("Could not find any leaders for docid %s ", docID))
	}
	return all, err
}

// GetClusterProps Intentionally return a copy vs a pointer want to be thread safe
func (s *solrZkInstance) GetClusterProps() (ClusterProps, error) {
	return s.zookeeper.GetClusterProps()
}

func (s *solrZkInstance) Listening() bool {
	return s.listening
}

func SolrZKLogger(logger Logger) func(s *solrZkInstance) {
	return func(solrZk *solrZkInstance) {
		solrZk.logger = logger
	}
}

func (s *solrZkInstance) GetReplicaUris() ([]string, error) {
	props, err := s.GetClusterProps()
	if err != nil {
		return []string{}, err
	}

	useHTTPS := props.UrlScheme == "https"

	protocol := "http"
	if useHTTPS {
		protocol = "https"
	}
	cs, err := s.GetClusterState()
	if err != nil {
		// technically this is never reached, s.GetClusterState always returns nil
		return []string{}, nil
	}
	nodes := cs.LiveNodes
	uris := make([]string, len(nodes))
	for i, v := range nodes {
		uris[i] = fmt.Sprintf("%s://%s/v2/c", protocol, v)
	}
	return shuffleNodes(uris), nil

}

func (s *solrZkInstance) GetShardFromRoute(route string) (string, error) {
	if strings.LastIndex(route, "!") != len(route)-1 {
		route += "!"
	}
	collection, ok := s.clusterState.Collections[s.collection]
	if !ok {
		return "", fmt.Errorf("[go-solr] Collection %s does not exist ", s.collection)
	}
	shard, err := findShard(route, &collection)
	if err != nil {
		return "", err
	}

	return shard.Name, nil
}

func (s *solrZkInstance) GetReplicasFromRoute(route string) ([]string, error) {
	if strings.LastIndex(route, "!") != len(route)-1 {
		route += "!"
	}
	collection, ok := s.clusterState.Collections[s.collection]
	if !ok {
		return nil, fmt.Errorf("[go-solr]  Collection %s does not exist ", s.collection)
	}
	//if contains route don't round robin
	hosts, err := findLiveReplicaUrls(route, &collection)
	if err != nil {
		return hosts, err
	}

	return shuffleNodes(hosts), nil

}
func shuffleNodes(nodes []string) []string {
	if len(nodes) == 1 {
		return nodes
	}
	dest := make([]string, len(nodes))
	perm := rand.Perm(len(nodes))
	for i, v := range perm {
		dest[v] = nodes[i]
	}
	return dest
}
