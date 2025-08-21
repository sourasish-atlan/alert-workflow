package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Grafana API request/response structures
type QueryRequest struct {
	From    string  `json:"from,omitempty"`
	To      string  `json:"to,omitempty"`
	Queries []Query `json:"queries"`
}

type Query struct {
	QueryText     string     `json:"queryText,omitempty"`
	RawSQL        string     `json:"rawSql,omitempty"`
	RefID         string     `json:"refId"`
	Datasource    DataSource `json:"datasource"`
	DatasourceID  int        `json:"datasourceId,omitempty"`
	MaxDataPoints int        `json:"maxDataPoints"`
	Format        string     `json:"format,omitempty"`
	IntervalMs    int        `json:"intervalMs,omitempty"`
	FillMode      string     `json:"fillMode,omitempty"`
	Hide          bool       `json:"hide,omitempty"`
	QueryType     string     `json:"queryType,omitempty"`
	TimeColumns   []string   `json:"timeColumns,omitempty"`
}

type DataSource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type QueryResponse struct {
	Results map[string]QueryResult `json:"results"`
}

type QueryResult struct {
	RefID  string      `json:"refId"`
	Frames []DataFrame `json:"frames,omitempty"`
	Error  string      `json:"error,omitempty"`
}

type DataFrame struct {
	Schema FrameSchema `json:"schema"`
	Data   FrameData   `json:"data"`
}

type FrameSchema struct {
	RefID  string      `json:"refId"`
	Fields []FieldInfo `json:"fields"`
}

type FieldInfo struct {
	Name     string                 `json:"name"`
	Type     string                 `json:"type"`
	TypeInfo map[string]interface{} `json:"typeInfo,omitempty"`
}

type FrameData struct {
	Values [][]interface{} `json:"values"`
}

// Business logic structures
type JoinedRecord struct {
	SalesforceID      string  `json:"salesforce_id"`
	SalesforceName    string  `json:"salesforce_name"`
	SalesforceType    string  `json:"salesforce_type"`
	Domain            string  `json:"domain"`
	TenantCreatedDate string  `json:"tenant_created_date"`
	CreatedByEmail    string  `json:"created_by_email"`
	CSM               string  `json:"csm"`
	Cost              float64 `json:"cost"`
}

type QueryResults struct {
	AccountData map[string][]string // salesforce_id -> [name, type]
	TenantData  map[string][]string // salesforce_id -> [domain, created_date, created_by_email]
	CSMData     map[string]string   // salesforce_id -> csm
	CostData    map[string]float64  // salesforce_id -> cost
}

func main() {
	// Setup logging with timestamps
	log.SetFlags(log.LstdFlags)

	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	serviceToken := os.Getenv("GRAFANA_SERVICE_TOKEN")
	if serviceToken == "" {
		log.Fatal("GRAFANA_SERVICE_TOKEN environment variable is required")
	}

	grafanaURL := os.Getenv("GRAFANA_URL")
	if grafanaURL == "" {
		grafanaURL = "https://observability.atlan.com"
	}

	log.Println("Starting Salesforce data extraction...")

	// Initialize results
	results := &QueryResults{
		AccountData: make(map[string][]string),
		TenantData:  make(map[string][]string),
		CSMData:     make(map[string]string),
		CostData:    make(map[string]float64),
	}

	// Fetch account details to get salesforce IDs
	log.Println("Fetching account details for deleted Salesforce accounts")
	accountData := fetchAccountData(grafanaURL, serviceToken, "")

	if len(accountData) == 0 {
		log.Println("No deleted accounts found")
		return
	}

	// Store account data and build IN clause for other queries
	var salesforceIDs []string
	for salesforceID, data := range accountData {
		results.AccountData[salesforceID] = data
		salesforceIDs = append(salesforceIDs, fmt.Sprintf("'%s'", salesforceID))
	}

	inClause := strings.Join(salesforceIDs, ",")
	log.Printf("Found %d deleted Salesforce accounts, fetching additional data", len(salesforceIDs))

	// Fetch remaining data
	fetchRemainingData(grafanaURL, serviceToken, inClause, results)

	// Perform left join and export
	joinedRecords := performLeftJoin(results)
	if len(joinedRecords) == 0 {
		log.Println("No records found after left join")
		return
	}

	// Save report
	if err := saveReportToFile(joinedRecords, "salesforce_report.txt"); err != nil {
		log.Printf("Error saving report: %v", err)
	} else {
		log.Printf("Saved report with %d records", len(joinedRecords))
	}

	log.Println("Data extraction completed successfully")
}

// Fetch account data from Snowflake
func fetchAccountData(grafanaURL, serviceToken, inClause string) map[string][]string {
	var queryText string
	if inClause == "" {
		queryText = `SELECT LOWER("ID") AS "salesforce_id", "NAME" AS "salesforce_name", "TYPE" AS "salesforce_type"
		FROM "SALESFORCE"."ACCOUNT" WHERE "TYPE" IN ('Lost Opportunity', 'Churned - Customer', 'Churned - Trial')`
	} else {
		queryText = fmt.Sprintf(`SELECT LOWER("ID") AS "salesforce_id", "NAME" AS "salesforce_name", "TYPE" AS "salesforce_type"
		FROM "SALESFORCE"."ACCOUNT" WHERE LOWER("ID") IN (%s)`, inClause)
	}

	req := QueryRequest{
		Queries: []Query{{
			QueryText:    queryText,
			RefID:        "A",
			Datasource:   DataSource{Type: "michelin-snowflake-datasource", UID: "ae62hcbfe169sf"},
			Format:       "table",
			DatasourceID: 31,
			//IntervalMs:    60000,
			//MaxDataPoints: 1547,
		}},
	}

	valuesArray := sendRequest(grafanaURL, serviceToken, req, "Account Details Query")
	result := make(map[string][]string)

	if len(valuesArray) >= 3 {
		for i := 0; i < len(valuesArray[0]); i++ {
			salesforceID := strings.ToLower(toString(valuesArray[0][i]))
			result[salesforceID] = []string{
				toString(valuesArray[1][i]), // name
				toString(valuesArray[2][i]), // type
			}
		}
	}

	return result
}

// Fetch remaining data (CSM, tenant, cost)
func fetchRemainingData(grafanaURL, serviceToken, inClause string, results *QueryResults) {
	// Fetch CSM data
	csmReq := QueryRequest{
		Queries: []Query{{
			QueryText: fmt.Sprintf(`SELECT DISTINCT LOWER("ID") AS "salesforce_id", "VITALLY_CSM_C" AS "csm"
			FROM "SALESFORCE"."ACCOUNT" WHERE LOWER("ID") IN (%s) AND "VITALLY_CSM_C" IS NOT NULL`, inClause),
			RefID:        "C",
			Datasource:   DataSource{Type: "michelin-snowflake-datasource", UID: "ae62hcbfe169sf"},
			Format:       "table",
			DatasourceID: 31,
			//IntervalMs:    60000,
			//MaxDataPoints: 1547,
		}},
	}

	if valuesArray := sendRequest(grafanaURL, serviceToken, csmReq, "CSM Information Query"); len(valuesArray) >= 2 {
		for i := 0; i < len(valuesArray[0]); i++ {
			salesforceID := strings.ToLower(toString(valuesArray[0][i]))
			results.CSMData[salesforceID] = toString(valuesArray[1][i])
		}
	}

	// Fetch tenant data
	csvFormat := strings.ReplaceAll(inClause, "'", "")
	tenantReq := QueryRequest{
		Queries: []Query{{
			RefID:      "B",
			Datasource: DataSource{Type: "grafana-postgresql-datasource", UID: "ceb9ek6fvijuoc"},
			RawSQL: fmt.Sprintf(`SELECT DISTINCT t.domain, LOWER(a.salesforce_account_id) AS salesforce_id, t.created_at AS tenant_created_date, ue_created.email AS created_by_email 
			FROM tenants t JOIN accounts a ON t.account_id = a.id LEFT JOIN user_entity ue_created ON ue_created.id = t.created_by::varchar WHERE 
			LOWER(a.salesforce_account_id) = ANY(SELECT unnest(string_to_array('%s', ','))) AND t.status = 'LIVE'`, csvFormat),
			Format:       "table",
			DatasourceID: 36,
		}},
	}

	if valuesArray := sendRequest(grafanaURL, serviceToken, tenantReq, "Tenant Information Query"); len(valuesArray) >= 4 {
		for i := 0; i < len(valuesArray[0]); i++ {
			salesforceID := strings.ToLower(toString(valuesArray[1][i]))
			results.TenantData[salesforceID] = []string{
				toString(valuesArray[0][i]),                      // domain
				formatUnixTimestamp(toString(valuesArray[2][i])), // created_date
				toString(valuesArray[3][i]),                      // created_by_email
			}
		}
	}

	// Fetch cost data
	costReq := QueryRequest{
		Queries: []Query{{
			Datasource: DataSource{Type: "michelin-snowflake-datasource", UID: "be6kvyuxuzgg0e"},
			FillMode:   "null",
			Hide:       false,
			QueryText: fmt.Sprintf(`SELECT SALESFORCE_ID, SUM(TENANTCOST) AS COST
			FROM "ATLAN_CLOUD"."PUBLIC"."MONTHLY_DASHBOARD_CUSTOMER_TENANT_COST_SALESFORCE_VIEW"
			WHERE DATE_TRUNC('MONTH', BILLING_DATE) = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE))
			AND LOWER("SALESFORCE_ID") IN (%s) GROUP BY SALESFORCE_ID`, inClause),
			Format:       "table",
			RefID:        "D",
			TimeColumns:  []string{"time"},
			DatasourceID: 34,
			//IntervalMs:    60000,
			//MaxDataPoints: 1547,
		}},
	}

	if valuesArray := sendRequest(grafanaURL, serviceToken, costReq, "Cost Data Query"); len(valuesArray) >= 2 {
		for i := 0; i < len(valuesArray[0]); i++ {
			salesforceID := strings.ToLower(toString(valuesArray[0][i]))
			results.CostData[salesforceID] = toFloat64(valuesArray[1][i])
		}
	}
}

// Perform left join from tenant data
func performLeftJoin(results *QueryResults) []JoinedRecord {
	var records []JoinedRecord

	for salesforceID, tenantData := range results.TenantData {
		record := JoinedRecord{
			SalesforceID:      salesforceID,
			Domain:            tenantData[0],
			TenantCreatedDate: tenantData[1],
			CreatedByEmail:    tenantData[2],
		}

		// Fill optional data if available
		if accountData, exists := results.AccountData[salesforceID]; exists && len(accountData) >= 2 {
			record.SalesforceName = accountData[0]
			record.SalesforceType = accountData[1]
		}

		if csm, exists := results.CSMData[salesforceID]; exists {
			record.CSM = csm
		}

		if cost, exists := results.CostData[salesforceID]; exists {
			record.Cost = cost
		}

		records = append(records, record)
	}

	return records
}

// Save formatted report to text file
func saveReportToFile(records []JoinedRecord, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create report file: %w", err)
	}
	defer file.Close()

	// Write header
	file.WriteString(strings.Repeat("=", 60) + "\n")
	file.WriteString("Live Churned Customers with Tenant Details:\n")
	file.WriteString(strings.Repeat("=", 60) + "\n")

	// Write records
	for _, record := range records {
		file.WriteString(fmt.Sprintf("- Name: %s\n", getValueOrNA(record.SalesforceName)))
		file.WriteString(fmt.Sprintf("- Domain: %s\n", getValueOrNA(record.Domain)))
		file.WriteString(fmt.Sprintf("- CSM: %s\n", getValueOrNA(record.CSM)))
		file.WriteString(fmt.Sprintf("- Creation_Time: %s\n", getValueOrNA(record.TenantCreatedDate)))
		file.WriteString(fmt.Sprintf("- Cost: %s\n", formatCostOrNA(record.Cost)))
		file.WriteString("---\n")
	}
	return nil
}

// Send HTTP request to Grafana API
func sendRequest(grafanaURL, serviceToken string, queryReq QueryRequest, requestName string) [][]interface{} {
	jsonData, err := json.Marshal(queryReq)
	if err != nil {
		log.Printf("Error marshaling %s: %v", requestName, err)
		return nil
	}

	log.Printf("Executing %s", requestName)

	req, err := http.NewRequestWithContext(context.Background(), "POST", fmt.Sprintf("%s/api/ds/query", grafanaURL), bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error creating %s: %v", requestName, err)
		return nil
	}

	req.Header.Set("Authorization", "Bearer "+serviceToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error sending %s: %v", requestName, err)
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading %s response: %v", requestName, err)
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Request %s failed with status: %s", requestName, resp.Status)
		log.Printf("Error response body: %s", string(body))
		return nil
	}

	var queryResp QueryResponse
	if err := json.Unmarshal(body, &queryResp); err != nil {
		log.Printf("Error parsing %s response: %v", requestName, err)
		return nil
	}

	// Map request name to RefID
	refIDMap := map[string]string{
		"Account Details Query":    "A",
		"CSM Information Query":    "C",
		"Tenant Information Query": "B",
		"Cost Data Query":          "D",
	}

	refID := refIDMap[requestName]
	if result, exists := queryResp.Results[refID]; exists && len(result.Frames) > 0 {
		valuesArray := result.Frames[0].Data.Values
		log.Printf("Completed %s: %d columns, %d rows", requestName, len(valuesArray), len(valuesArray[0]))
		return valuesArray
	}

	log.Printf("No data found in %s response", requestName)
	return nil
}

// Utility functions
func toString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func toFloat64(value interface{}) float64 {
	if value == nil {
		return 0.0
	}

	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		if result, err := strconv.ParseFloat(v, 64); err == nil {
			return result
		}
	}
	return 0.0
}

func getValueOrNA(value string) string {
	if value == "" {
		return "N/A"
	}
	return value
}

func formatCostOrNA(cost float64) string {
	if cost == 0.0 {
		return "N/A"
	}
	return fmt.Sprintf("$%.2f", cost)
}

func formatUnixTimestamp(timestampStr string) string {
	if timestampStr == "" {
		return "N/A"
	}

	// Parse as float to handle scientific notation
	if timestamp, err := strconv.ParseFloat(timestampStr, 64); err == nil {
		timestampInt := int64(timestamp)

		// Check if it's in milliseconds
		if timestampInt > 1000000000000 {
			return time.Unix(timestampInt/1000, 0).UTC().Format("2006-01-02")
		}
	}

	// Fallback: try to extract date from existing format
	if len(timestampStr) >= 10 {
		return timestampStr[:10]
	}

	return timestampStr
}
