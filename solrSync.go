package solr

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func (s *solrZkInstance) Listen() error {
	err := s.zookeeper.Connect()
	s.zookeeper.ZKLogger(s.logger)
	s.currentNode = 0
	if err != nil {
		return err
	}
	s.clusterState = ClusterState{}
	var collectionsEvents <-chan zk.Event
	var liveNodeEvents <-chan zk.Event
	connect := func() error {
		collectionsEvents, err = s.initCollectionsListener()
		if err != nil {
			return err
		}
		liveNodeEvents, err = s.initLiveNodesListener()
		return err
	}
	err = connect()
	if err != nil {
		return err
	}
	//loop forever
	go func() {
		log := s.logger
		sleepTime := s.sleepTimeMS
		logErr := func(err error) {
			log.Error(fmt.Errorf("[go-solr] Error connecting to zk %v sleeping: %d", err, sleepTime))
		}
		for {
			shouldReconnect := false
			select {
			case cEvent := <-collectionsEvents:
				if cEvent.Err != nil {
					log.Debug(fmt.Sprintf("[go-solr] error on cevent %v", cEvent))
					logErr(cEvent.Err)
					shouldReconnect = isConnectionClosed(err)
					sleepTime = backoff(sleepTime)
					break
				}
				// do something if its not a session or disconnect
				if cEvent.Type == zk.EventNodeDataChanged {
					collections, version, err := s.zookeeper.GetClusterState()
					if err != nil {
						logErr(err)
						sleepTime = backoff(sleepTime)
						continue
					}
					s.setCollections(collections, version)
				}
				sleepTime = s.sleepTimeMS

			case nEvent := <-liveNodeEvents:
				if nEvent.Err != nil {
					logErr(nEvent.Err)
					shouldReconnect = isConnectionClosed(err)
					log.Error(fmt.Errorf("[go-solr] error on nevent %v", nEvent))
					sleepTime = backoff(sleepTime)
					break
				}
				// do something if its not a session or disconnect
				if nEvent.Type == zk.EventNodeDataChanged || nEvent.Type == zk.EventNodeChildrenChanged {
					liveNodes, err := s.zookeeper.GetLiveNodes()
					if err != nil {
						logErr(err)
						sleepTime = backoff(sleepTime)

						continue
					}
					s.setLiveNodes(liveNodes)
				}
				sleepTime = s.sleepTimeMS
			}
			if shouldReconnect {
				err = connect()
				if err != nil {
					s.logger.Error(fmt.Errorf("[go-solr] zk connect err %v, sleeping %d", err, sleepTime))
					sleepTime = backoff(sleepTime)
				} else {
					sleepTime = s.sleepTimeMS
				}
			}
		}
	}()
	s.listening = true
	return nil
}
func isConnectionClosed(err error) bool {
	return err == zk.ErrClosing || err == zk.ErrConnectionClosed
}
func backoff(sleepTime int) int {
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	return sleepTime * 2
}

func (s *solrZkInstance) initCollectionsListener() (<-chan zk.Event, error) {
	collections, version, collectionsEvents, err := s.zookeeper.GetClusterStateW()
	if err != nil {
		return nil, err
	}
	s.setCollections(collections, version)
	return collectionsEvents, nil
}

func (s *solrZkInstance) initLiveNodesListener() (<-chan zk.Event, error) {
	liveNodes, liveNodeEvents, err := s.zookeeper.GetLiveNodesW()
	if err != nil {
		return nil, err
	}
	s.setLiveNodes(liveNodes)
	return liveNodeEvents, nil
}

// GetClusterState Intentionally return a copy vs a pointer want to be thread safe
func (s *solrZkInstance) GetClusterState() (ClusterState, error) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	return s.clusterState, nil
}

func (s *solrZkInstance) setLiveNodes(nodes []string) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	s.clusterState.LiveNodes = nodes
	s.logger.Debug(fmt.Sprintf("go-solr: zk livenodes updated %v ", s.clusterState.LiveNodes))
}

func (s *solrZkInstance) setCollections(collections map[string]Collection, version int) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	s.clusterState.Collections = collections
	s.clusterState.Version = version
	s.logger.Debug(fmt.Sprintf("go-solr: zk collections updated %v ", s.clusterState.Collections))
}
