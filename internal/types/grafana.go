package types

// Grafana API structures
type QueryRequest struct {
	Queries []Query `json:"queries"`
}

type Query struct {
	QueryText    string     `json:"queryText,omitempty"`
	RawSQL       string     `json:"rawSql,omitempty"`
	RefID        string     `json:"refId"`
	Datasource   DataSource `json:"datasource"`
	DatasourceID int        `json:"datasourceId,omitempty"`
	Format       string     `json:"format,omitempty"`
	FillMode     string     `json:"fillMode,omitempty"`
	Hide         bool       `json:"hide,omitempty"`
	TimeColumns  []string   `json:"timeColumns,omitempty"`
	From         string     `json:"from,omitempty"`
	To           string     `json:"to,omitempty"`
}

type DataSource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type QueryResponse struct {
	Results map[string]QueryResult `json:"results"`
}

type QueryResult struct {
	Frames []DataFrame `json:"frames,omitempty"`
}

type DataFrame struct {
	Data FrameData `json:"data"`
}

type FrameData struct {
	Values [][]interface{} `json:"values"`
}
