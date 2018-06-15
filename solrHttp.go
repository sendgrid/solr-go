package solr

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

type solrHttp struct {
	user                  string
	password              string
	queryClient           HTTPer
	writeClient           HTTPer
	solrZk                SolrZK
	collection            string
	cert                  string
	defaultRows           uint32
	minRf                 int
	logger                Logger
	insecureSkipVerify    bool
	writeTimeoutSeconds   int
	readTimeoutSeconds    int
	connectTimeoutSeconds int
	router                Router
}

func NewSolrHTTP(useHTTPS bool, collection string, options ...func(*solrHttp)) (SolrHTTP, error) {
	solrCli := solrHttp{collection: collection, minRf: 1, insecureSkipVerify: false, readTimeoutSeconds: 20, writeTimeoutSeconds: 30, connectTimeoutSeconds: 5}
	logger := log.New(os.Stdout, "[SolrClient] ", log.LstdFlags)
	solrCli.logger = &SolrLogger{logger}
	for _, opt := range options {
		opt(&solrCli)
	}

	var err error
	if solrCli.writeClient == nil {
		solrCli.writeClient, err = getClient(solrCli.cert, useHTTPS, solrCli.insecureSkipVerify, solrCli.writeTimeoutSeconds, solrCli.connectTimeoutSeconds)
		if err != nil {
			return nil, err
		}
	}

	if solrCli.queryClient == nil {
		solrCli.queryClient, err = getClient(solrCli.cert, useHTTPS, solrCli.insecureSkipVerify, solrCli.readTimeoutSeconds, solrCli.connectTimeoutSeconds)
		if err != nil {
			return nil, err
		}
	}

	if solrCli.router == nil {
		solrCli.router = NewRoundRobinRouter()
	}

	return &solrCli, nil
}

func (s *solrHttp) Update(nodeUris []string, singleDoc bool, doc interface{}, opts ...func(url.Values)) error {
	if len(nodeUris) == 0 {
		return fmt.Errorf("[SolrHTTP] nodeuris: empty node uris is not valid")
	}
	nodeUri := nodeUris[0]
	urlVals := url.Values{
		"min_rf": {fmt.Sprintf("%d", s.minRf)},
	}

	for _, opt := range opts {
		opt(urlVals)
	}

	uri := fmt.Sprintf("%s/%s/update", nodeUri, s.collection)
	if singleDoc {
		uri += "/json/docs"
	}
	var buf bytes.Buffer
	if doc != nil {
		enc := json.NewEncoder(&buf)
		if err := enc.Encode(doc); err != nil {
			return err
		}
	}
	req, err := http.NewRequest("POST", uri, &buf)

	if err != nil {
		return err
	}
	req.URL.RawQuery = urlVals.Encode()

	req.Header.Add("Content-Type", "application/json")
	basicCred := s.getBasicCredential(s.user, s.password)
	if basicCred != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", basicCred))
	}

	start := time.Now()
	resp, err := s.writeClient.Do(req)
	if s.router != nil {
		s.router.AddSearchResult(time.Since(start), nodeUri, resp.StatusCode, err)
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		htmlData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response body for StatusCode %d, err: %s", resp.StatusCode, err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if resp.StatusCode < 500 {
			return NewSolrError(resp.StatusCode, string(htmlData))
		} else {
			return NewSolrInternalError(resp.StatusCode, string(htmlData))
		}
	}
	var r UpdateResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&r); err != nil {
		return NewSolrParseError(resp.StatusCode, err.Error())
	}

	if r.Response.Status != 0 {
		msg := r.Error.Msg
		return NewSolrError(r.Response.Status, msg)
	}

	if r.Response.RF < r.Response.MinRF {
		return NewSolrRFError(r.Response.RF, r.Response.MinRF)
	}
	return nil
}

func (s *solrHttp) Select(nodeUris []string, opts ...func(url.Values)) (SolrResponse, error) {
	if len(nodeUris) == 0 {
		return SolrResponse{}, fmt.Errorf("[SolrHTTP] nodeuris: empty node uris is not valid")
	}
	nodeUri := s.router.GetUriFromList(nodeUris)

	var err error
	urlValues := url.Values{
		"wt": {"json"},
	}
	for _, opt := range opts {
		opt(urlValues)
	}
	var sr SolrResponse
	u := fmt.Sprintf("%s/%s/select", nodeUri, s.collection)
	body := bytes.NewBufferString(urlValues.Encode())
	req, err := http.NewRequest("POST", u, body)
	if err != nil {
		return sr, err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	basicCred := s.getBasicCredential(s.user, s.password)
	if basicCred != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", basicCred))
	}
	start := time.Now()
	resp, err := s.queryClient.Do(req)
	if s.router != nil {
		s.router.AddSearchResult(time.Since(start), nodeUri, resp.StatusCode, err)
	}
	if err != nil {
		return sr, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		sr.Status = 404
		return sr, ErrNotFound
	}
	if resp.StatusCode >= 400 {
		htmlData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return sr, err
		}
		sr.Status = resp.StatusCode
		return sr, NewSolrError(resp.StatusCode, string(htmlData))
	}

	dec := json.NewDecoder(resp.Body)

	return sr, dec.Decode(&sr)
}

func getMapChunks(in []map[string]interface{}, chunkSize int) [][]map[string]interface{} {
	var out [][]map[string]interface{}
	for i := 0; i < len(in); i += chunkSize {
		end := i + chunkSize
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[i:end])
	}
	return out
}

func (s *solrHttp) Logger() Logger {
	return s.logger
}

func getidChunks(in []string, chunkSize int) [][]string {
	var out [][]string
	for i := 0; i < len(in); i += chunkSize {
		end := i + chunkSize
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[i:end])
	}
	return out
}

func DeleteStreamBody(filter string) func(url.Values) {
	return func(p url.Values) {
		p["stream.body"] = []string{fmt.Sprintf("<delete><query>%s</query></delete>", filter)}
	}
}

func Query(q string) func(url.Values) {
	return func(p url.Values) {
		p["q"] = []string{q}
	}
}

func ClusterStateVersion(version int, collection string) func(url.Values) {
	return func(p url.Values) {
		p["_stateVer_"] = []string{fmt.Sprintf("%s:%d", collection, version)}
	}
}

//Helper funcs for setting the solr query params
func FilterQuery(fq string) func(url.Values) {
	return func(p url.Values) {
		p["fq"] = []string{fq}
	}
}

func Rows(rows uint32) func(url.Values) {
	return func(p url.Values) {
		p["rows"] = []string{strconv.FormatUint(uint64(rows), 10)}
	}
}

func Route(r string) func(url.Values) {
	return func(p url.Values) {
		if r != "" {
			p["_route_"] = []string{r}
		}
	}
}

func PreferLocalShards(preferLocalShards bool) func(url.Values) {
	return func(p url.Values) {
		if preferLocalShards {
			p["preferLocalShards"] = []string{"true"}
		}
	}
}

func Start(start uint32) func(url.Values) {
	return func(p url.Values) {
		p["start"] = []string{strconv.FormatUint(uint64(start), 10)}
	}
}

func Sort(s string) func(url.Values) {
	return func(p url.Values) {
		p["sort"] = []string{s}
	}
}

func Commit(commit bool) func(url.Values) {
	return func(p url.Values) {
		commitString := "false"
		if commit {
			commitString = "true"
		}
		p["commit"] = []string{commitString}
	}
}

func Cursor(c string) func(url.Values) {
	return func(p url.Values) {
		p["cursorMark"] = []string{c}
	}
}

func UrlVals(urlVals url.Values) func(url.Values) {
	return func(p url.Values) {
		for key := range urlVals {
			p[key] = urlVals[key]
		}
	}
}

func getClient(cert string, https bool, insecureSkipVerify bool, timeoutSeconds int, connectTimeoutSeconds int) (HTTPer, error) {
	connectTimeout := time.Duration(connectTimeoutSeconds) * time.Second
	cli := &http.Client{
		Timeout: time.Duration(timeoutSeconds) * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			DialContext:         (&net.Dialer{Timeout: connectTimeout}).DialContext},
	}
	if https {
		tlsConfig, err := getTLSConfig(cert, insecureSkipVerify)
		if err != nil {
			return nil, err
		}
		cli.Transport = &http.Transport{
			TLSClientConfig:     tlsConfig,
			MaxIdleConnsPerHost: 10,
			DialContext:         (&net.Dialer{Timeout: connectTimeout}).DialContext,
		}
	}
	return cli, nil
}

func getTLSConfig(certPath string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConf := &tls.Config{InsecureSkipVerify: insecureSkipVerify}
	if certPath != "" {
		zkRootPEM, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, err
		}

		zkRoots := x509.NewCertPool()
		ok := zkRoots.AppendCertsFromPEM([]byte(zkRootPEM))
		if !ok {
			log.Fatal("failed to parse zkRoot certificate")
		}
		tlsConf.RootCAs = zkRoots
	}
	return tlsConf, nil
}

func (s *solrHttp) getBasicCredential(user string, password string) string {
	if user != "" {
		userPass := fmt.Sprintf("%s:%s", user, password)
		return b64.StdEncoding.EncodeToString([]byte(userPass))
	}
	return ""
}

//HTTPClient sets the HTTPer
func HTTPClient(cli HTTPer) func(*solrHttp) {
	return func(c *solrHttp) {
		c.queryClient = cli
		c.writeClient = cli
	}
}

//DefaultRows sets number of rows for pagination
//in calls that don't pass a number of rows in
func DefaultRows(rows uint32) func(*solrHttp) {
	return func(c *solrHttp) {
		c.defaultRows = rows
	}
}

//The path to tls certificate (optional)
func Cert(cert string) func(*solrHttp) {
	return func(c *solrHttp) {
		c.cert = cert
	}
}

func User(user string) func(*solrHttp) {
	return func(c *solrHttp) {
		c.user = user
	}
}

func Password(password string) func(*solrHttp) {
	return func(c *solrHttp) {
		c.password = password
	}
}

func QueryRouter(router Router) func(*solrHttp) {
	return func(c *solrHttp) {
		c.router = router
	}
}

func MinRF(minRf int) func(*solrHttp) {
	return func(c *solrHttp) {
		c.minRf = minRf
	}
}

func WriteTimeout(seconds int) func(*solrHttp) {
	return func(c *solrHttp) {
		c.writeTimeoutSeconds = seconds
	}
}

func ReadTimeout(seconds int) func(*solrHttp) {
	return func(c *solrHttp) {
		c.readTimeoutSeconds = seconds
	}
}

func ConnectionTimeout(seconds int) func(*solrHttp) {
	return func(c *solrHttp) {
		c.connectTimeoutSeconds = seconds
	}
}

func InsecureSkipVerify(insecureSkipVerify bool) func(*solrHttp) {
	return func(c *solrHttp) {
		c.insecureSkipVerify = insecureSkipVerify
	}
}

func HttpLogger(logger Logger) func(*solrHttp) {
	return func(c *solrHttp) {
		c.logger = logger
	}
}
