package solr

const (
	activeState     string = "active"
	recoveringState string = "recovering"
)

func findLeader(key string, cs *Collection) (string, error) {
	replicas, err := findReplicas(key, cs)
	if err != nil {
		return "", err
	}
	return findLeaderFromReplicas(replicas), nil
}

func findLiveReplicaUrls(key string, cs *Collection) ([]string, error) {
	replicas, err := findReplicas(key, cs)
	if err != nil {
		return nil, err
	}
	var replicaUrls []string = make([]string, 0, len(replicas))
	for _, replica := range replicas {
		if isReplicaActive(&replica) {
			replicaUrls = append(replicaUrls, replica.BaseURL)
		}
	}
	return replicaUrls, nil
}

func findShard(key string, cs *Collection) (*Shard, error) {
	composite, err := NewCompositeKey(key)
	if err != nil {
		return nil, err
	}
	shardKeyHash := Hash(composite)
	for name, shard := range cs.Shards {
		if isShardActive(&shard) {
			hashRange, err := ConvertToHashRange(shard.Range)
			if err != nil {
				return nil, err
			}
			if shardKeyHash >= hashRange.Low && shardKeyHash <= hashRange.High {
				shard.Name = name
				return &shard, nil
			}
		}
	}
	return nil, ErrNotFound
}

func findReplicas(key string, cs *Collection) (map[string]Replica, error) {
	shard, err := findShard(key, cs)
	if err != nil {
		return nil, err
	}
	replicas := make(map[string]Replica)
	for k, v := range shard.Replicas {
		if isReplicaActive(&v) {
			replicas[k] = v
		}
	}
	return replicas, nil
}

func isReplicaActive(r *Replica) bool {
	return r.State == recoveringState || r.State == activeState
}

func isShardActive(s *Shard) bool {
	return s.State == activeState
}

func findLeaderFromReplicas(replicas map[string]Replica) string {
	var leader string
	for _, replica := range replicas {
		if replica.Leader == "true" && isReplicaActive(&replica) {
			leader = replica.BaseURL
			break
		}
	}
	return leader
}
