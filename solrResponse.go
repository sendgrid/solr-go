package solr

type SolrResponse struct {
	Status int `json:"status"`
	QTime  int `json:"qtime"`
	Params struct {
		Query  string `json:"q"`
		Indent string `json:"indent"`
		Wt     string `json:"wt"`
	} `json:"params"`
	Response       Response `json:"response"`
	NextCursorMark string   `json:"nextCursorMark"`
	Adds           Adds     `json:"adds"`
}

type Response struct {
	NumFound uint32                   `json:"numFound"`
	Start    int                      `json:"start"`
	Docs     []map[string]interface{} `json:"docs"`
}

func GetDocIdFromDoc(m map[string]interface{}) string {
	if v, ok := m["id"]; ok {
		return v.(string)
	}
	return ""
}

func GetVersionFromDoc(m map[string]interface{}) int {
	if v, ok := m["_version_"]; ok {
		switch v := v.(type) {
		case float64:
			return int(v)
		case int:
			return v
		}
	}

	return 0
}

type Adds map[string]int

type UpdateResponse struct {
	Response struct {
		Status int `json:"status"`
		QTime  int `json:"QTime"`
		RF     int `json:"rf"`
		MinRF  int `json:"min_rf"`
	} `json:"responseHeader"`
	Error struct {
		Metadata []string `json:"metadata"`
		Msg      string   `json:"msg"`
		Code     int      `json:"code"`
	}
}

type DeleteRequest struct {
	Delete []string `json:"delete"`
}
