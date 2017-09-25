package solr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"time"
)

type zookeeper struct {
	connectionString string
	zkConnection     *zk.Conn
	collection       string
	zkRoot           string
	pollSleep        time.Duration
}

type stateChanged func([]byte, error)

type Zookeeper interface {
	IsConnected() bool
	Connect() error
	GetConnectionString() string
	Get(path string) ([]byte, int, error)
	Poll(path string, cb stateChanged)
	GetClusterState() (map[string]Collection, int, error)
	GetClusterStateW() (map[string]Collection, int, <-chan zk.Event, error)
	GetLiveNodes() ([]string, error)
	GetLiveNodesW() ([]string, <-chan zk.Event, error)
	GetLeaderElectW() (<-chan zk.Event, error)
	GetClusterProps() (ClusterProps, error)
	ZKLogger(l Logger)
}

func NewZookeeper(connectionString string, zkRoot string, collection string) Zookeeper {
	return &zookeeper{connectionString: connectionString, zkRoot: zkRoot, collection: collection, pollSleep: time.Duration(1) * time.Second}
}

func (z *zookeeper) Connect() error {
	servers := strings.Split(z.connectionString, ",")
	zkConnection, _, err := zk.Connect(servers, time.Second) //*10)
	if err != nil {
		return err
	}
	z.zkConnection = zkConnection
	return nil
}
func (z *zookeeper) ZKLogger(l Logger) {
	if z.zkConnection != nil {
		z.zkConnection.SetLogger(l)
	}
}
func (z *zookeeper) IsConnected() bool {
	if z.zkConnection == nil {
		return false
	}
	return z.zkConnection.State() != zk.StateDisconnected &&
		z.zkConnection.State() != zk.StateExpired &&
		z.zkConnection.State() != zk.StateAuthFailed
}

func (z *zookeeper) Get(node string) ([]byte, int, error) {
	bytes, stat, err := z.zkConnection.Get(node)
	if err != nil {
		return nil, 0, err
	}
	val := bytes[:]
	return val, int(stat.Version), nil
}

func (z *zookeeper) GetConnectionString() string {
	return z.connectionString
}

func (z *zookeeper) Poll(path string, cb stateChanged) {
	for {
		bytes, _, err := z.Get(path)
		cb(bytes, err)
		time.Sleep(z.pollSleep)
	}
}

func (z *zookeeper) GetClusterStateW() (map[string]Collection, int, <-chan zk.Event, error) {
	node, stat, events, err := z.zkConnection.GetW(z.getClusterStatePath(z.zkRoot, z.collection))
	if err != nil {
		return nil, 0, events, err
	}
	cs, err := deserializeClusterState(node)
	if err != nil {
		return cs, 0, events, err
	}
	return cs, int(stat.Version), events, nil
}

func (z *zookeeper) GetClusterState() (map[string]Collection, int, error) {
	node, stat, err := z.zkConnection.Get(z.getClusterStatePath(z.zkRoot, z.collection))
	if err != nil {
		return nil, 0, err
	}
	cs, err := deserializeClusterState(node)
	if err != nil {
		return cs, 0, err
	}
	return cs, int(stat.Version), nil
}

func (z *zookeeper) GetLeaderElectW() (<-chan zk.Event, error) {
	_, _, events, err := z.zkConnection.GetW(fmt.Sprintf("/%s/collections/%s/leader_elect", z.zkRoot, z.collection))
	if err != nil {
		return events, err
	}

	return events, nil
}

func (z *zookeeper) GetClusterProps() (ClusterProps, error) {
	node, _, err := z.zkConnection.Get(fmt.Sprintf("/%s/clusterprops.json", z.zkRoot))
	if err != nil {
		if strings.Contains(err.Error(), "zk: node does not exist") {
			return ClusterProps{UrlScheme: "http"}, nil
		}
		return ClusterProps{}, err
	}
	cp, err := deserializeClusterProps(node)
	if err != nil {
		return cp, err
	}
	return cp, nil
}

func (z *zookeeper) GetLiveNodesW() ([]string, <-chan zk.Event, error) {
	children, _, events, err := z.zkConnection.ChildrenW(z.getLiveNodesPath(z.zkRoot))
	if err != nil {
		return children, events, err
	}
	for i, node := range children {
		children[i] = strings.Replace(node, "_solr", "", -1)
	}
	return children, events, nil
}

func (z *zookeeper) GetLiveNodes() ([]string, error) {
	children, _, err := z.zkConnection.Children(z.getLiveNodesPath(z.zkRoot))
	if err != nil {
		return children, err
	}
	for i, node := range children {
		children[i] = strings.Replace(node, "_solr", "", -1)
	}
	return children, nil
}

func (z *zookeeper) getLiveNodesPath(root string) string {
	return fmt.Sprintf("/%s/live_nodes", root)
}

func deserializeClusterState(node []byte) (map[string]Collection, error) {
	var collections map[string]Collection
	decoder := json.NewDecoder(bytes.NewBuffer(node))
	if err := decoder.Decode(&collections); err != nil {
		return nil, err
	}
	return collections, nil
}

func deserializeClusterProps(node []byte) (ClusterProps, error) {
	var clusterProps ClusterProps
	decoder := json.NewDecoder(bytes.NewBuffer(node))
	if err := decoder.Decode(&clusterProps); err != nil {
		return ClusterProps{}, err
	}
	return clusterProps, nil
}

func (z *zookeeper) getClusterStatePath(root string, collection string) string {
	return fmt.Sprintf("/%s/collections/%s/state.json", root, collection)
}
