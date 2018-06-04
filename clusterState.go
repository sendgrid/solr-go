package solr

type ClusterState struct {
	LiveNodes   []string
	Version     int
	Collections map[string]Collection
}

type Collection struct {
	Shards            map[string]Shard `json:"shards"`
	ReplicationFactor string           `json:"replicationFactor"`
}

type Shard struct {
	Name     string             `json:"-"`
	Range    string             `json:"range"`
	State    string             `json:"state"`
	Replicas map[string]Replica `json:"replicas"`
}

type Replica struct {
	Core     string `json:"core"`
	Leader   string `json:"leader"`
	BaseURL  string `json:"base_url"`
	NodeName string `json:"node_name"`
	State    string `json:"state"`
}

type SolrHealthcheckResponse struct {
	LiveNodes              []string `json:"live_nodes"`
	DownReplicas           []string `json:"down_replicas"`
	RecoveryFailedReplicas []string `json:"recovery_failed_replicas"`
}
