package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

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

// Data structures
type JoinedRecord struct {
	SalesforceID      string
	SalesforceName    string
	SalesforceType    string
	Domain            string
	TenantCreatedDate string
	CreatedByEmail    string
	CSM               string
	Cost              float64
}

// Report structures
type ChurnReport struct {
	Metadata        ReportMetadata   `json:"metadata"`
	Summary         ReportSummary    `json:"summary"`
	ChurnedAccounts []ChurnedAccount `json:"churned_accounts"`
}

type ReportMetadata struct {
	ReportType  string `json:"report_type"`
	GeneratedAt string `json:"generated_at"`
	GeneratedBy string `json:"generated_by"`
}

type ReportSummary struct {
	TotalAccounts        int            `json:"total_accounts"`
	AccountsByType       map[string]int `json:"accounts_by_type"`
	TotalMonthlyCost     float64        `json:"total_monthly_cost"`
	AverageCostPerTenant float64        `json:"average_cost_per_tenant"`
}

type ChurnedAccount struct {
	Name         *string  `json:"name"`
	Type         *string  `json:"type"`
	Domain       *string  `json:"domain"`
	CreationDate *string  `json:"creation_date"`
	CreatedBy    *string  `json:"created_by_email"`
	CSM          *string  `json:"csm"`
	Cost         *float64 `json:"cost"`
}

type QueryResults struct {
	AccountData map[string][]string // salesforce_id -> [name, type]
	TenantData  map[string][]string // salesforce_id -> [domain, created_date, created_by_email]
	CSMData     map[string]string   // salesforce_id -> csm
	CostData    map[string]float64  // salesforce_id -> cost
}

func main() {
	// Setup zap logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	// Load environment variables
	if err := godotenv.Load(); err != nil {
		sugar.Info("No .env file found, using environment variables")
	}

	serviceToken := os.Getenv("GRAFANA_SERVICE_TOKEN")
	if serviceToken == "" {
		sugar.Fatal("GRAFANA_SERVICE_TOKEN environment variable is required")
	}

	grafanaURL := os.Getenv("GRAFANA_URL")
	if grafanaURL == "" {
		grafanaURL = "https://observability.atlan.com"
	}

	sugar.Info("Starting Salesforce data extraction...")

	// Initialize results
	results := &QueryResults{
		AccountData: make(map[string][]string),
		TenantData:  make(map[string][]string),
		CSMData:     make(map[string]string),
		CostData:    make(map[string]float64),
	}

	// This will be replaced with Temporal Workflow logic
	sugar.Info("Fetching Salesforce churn data...")

	// For now, keep the original logic (will be converted to Workflow)
	accountData, err := fetchAccountDataSync(grafanaURL, serviceToken)
	if err != nil {
		sugar.Errorf("Error fetching account data: %v", err)
		return
	}

	results.AccountData = accountData

	// Build IN clause for other queries
	var salesforceIDs []string
	for id := range accountData {
		salesforceIDs = append(salesforceIDs, fmt.Sprintf("'%s'", id))
	}
	inClause := strings.Join(salesforceIDs, ",")
	sugar.Infof("Found %d churned accounts, fetching additional data", len(salesforceIDs))

	// Fetch remaining data
	if csmData, err := fetchCSMDataSync(grafanaURL, serviceToken, inClause); err == nil {
		results.CSMData = csmData
	}
	if tenantData, err := fetchTenantDataSync(grafanaURL, serviceToken, inClause); err == nil {
		results.TenantData = tenantData
	}
	if costData, err := fetchCostDataSync(grafanaURL, serviceToken, inClause); err == nil {
		results.CostData = costData
	}

	// Generate and save report
	joinedRecords := performLeftJoin(results)
	if len(joinedRecords) == 0 {
		sugar.Info("No records found")
		return
	}

	// Generate timestamped filename
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("report_%s.json", timestamp)

	if err := saveReportSync(joinedRecords, filename); err != nil {
		sugar.Errorf("Error saving report: %v", err)
	} else {
		sugar.Infof("Saved report with %d records to %s", len(joinedRecords), filename)
	}

	sugar.Info("Data extraction completed successfully")
}

// Activity: Fetch churned Salesforce account data
func FetchAccountDataActivity(ctx context.Context, grafanaURL, serviceToken string) (map[string][]string, error) {
	accountQuery := `SELECT LOWER("ID") AS "salesforce_id", "NAME" AS "salesforce_name", "TYPE" AS "salesforce_type"
		FROM "SALESFORCE"."ACCOUNT" WHERE "TYPE" IN ('Lost Opportunity', 'Churned - Customer', 'Churned - Trial')`

	accountReq := QueryRequest{
		Queries: []Query{{
			QueryText:    accountQuery,
			RefID:        "A",
			Datasource:   DataSource{Type: "michelin-snowflake-datasource", UID: "ae62hcbfe169sf"},
			Format:       "table",
			DatasourceID: 31,
		}},
	}

	valuesArray := sendRequest(grafanaURL, serviceToken, accountReq, "Account Data")
	if len(valuesArray) < 3 {
		return nil, fmt.Errorf("no account data found")
	}

	result := make(map[string][]string)
	for i := 0; i < len(valuesArray[0]); i++ {
		salesforceID := strings.ToLower(toString(valuesArray[0][i]))
		result[salesforceID] = []string{
			toString(valuesArray[1][i]), // name
			toString(valuesArray[2][i]), // type
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no churned accounts found")
	}

	return result, nil
}

// Activity: Fetch CSM data for given Salesforce IDs
func FetchCSMDataActivity(ctx context.Context, grafanaURL, serviceToken, inClause string) (map[string]string, error) {
	csmQuery := fmt.Sprintf(`SELECT DISTINCT LOWER("ID") AS "salesforce_id", "VITALLY_CSM_C" AS "csm"
		FROM "SALESFORCE"."ACCOUNT" WHERE LOWER("ID") IN (%s) AND "VITALLY_CSM_C" IS NOT NULL`, inClause)
	csmReq := QueryRequest{Queries: []Query{{QueryText: csmQuery, RefID: "C", Datasource: DataSource{Type: "michelin-snowflake-datasource", UID: "ae62hcbfe169sf"}, Format: "table", DatasourceID: 31}}}

	valuesArray := sendRequest(grafanaURL, serviceToken, csmReq, "CSM Data")
	if len(valuesArray) < 2 {
		return make(map[string]string), nil // Return empty map, not an error
	}

	result := make(map[string]string)
	for i := 0; i < len(valuesArray[0]); i++ {
		result[strings.ToLower(toString(valuesArray[0][i]))] = toString(valuesArray[1][i])
	}

	return result, nil
}

// Activity: Fetch tenant data for given Salesforce IDs
func FetchTenantDataActivity(ctx context.Context, grafanaURL, serviceToken, inClause string) (map[string][]string, error) {
	csvFormat := strings.ReplaceAll(inClause, "'", "")
	tenantQuery := fmt.Sprintf(`SELECT DISTINCT t.domain, LOWER(a.salesforce_account_id) AS salesforce_id, t.created_at AS tenant_created_date, ue_created.email AS created_by_email 
		FROM tenants t JOIN accounts a ON t.account_id = a.id LEFT JOIN user_entity ue_created ON ue_created.id = t.created_by::varchar WHERE 
		LOWER(a.salesforce_account_id) = ANY(SELECT unnest(string_to_array('%s', ','))) AND t.status = 'LIVE'`, csvFormat)
	tenantReq := QueryRequest{Queries: []Query{{RawSQL: tenantQuery, RefID: "B", Datasource: DataSource{Type: "grafana-postgresql-datasource", UID: "ceb9ek6fvijuoc"}, Format: "table", DatasourceID: 36}}}

	valuesArray := sendRequest(grafanaURL, serviceToken, tenantReq, "Tenant Data")
	if len(valuesArray) < 4 {
		return make(map[string][]string), nil // Return empty map, not an error
	}

	result := make(map[string][]string)
	for i := 0; i < len(valuesArray[0]); i++ {
		salesforceID := strings.ToLower(toString(valuesArray[1][i]))
		result[salesforceID] = []string{
			toString(valuesArray[0][i]),
			formatTimestamp(toString(valuesArray[2][i])),
			toString(valuesArray[3][i]),
		}
	}

	return result, nil
}

// Activity: Fetch cost data for given Salesforce IDs
func FetchCostDataActivity(ctx context.Context, grafanaURL, serviceToken, inClause string) (map[string]float64, error) {
	costQuery := fmt.Sprintf(`SELECT SALESFORCE_ID, SUM(TENANTCOST) AS COST FROM "ATLAN_CLOUD"."PUBLIC"."MONTHLY_DASHBOARD_CUSTOMER_TENANT_COST_SALESFORCE_VIEW"
		WHERE DATE_TRUNC('MONTH', BILLING_DATE) = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE)) AND LOWER("SALESFORCE_ID") IN (%s) GROUP BY SALESFORCE_ID`, inClause)
	costReq := QueryRequest{Queries: []Query{{QueryText: costQuery, RefID: "D", Datasource: DataSource{Type: "michelin-snowflake-datasource", UID: "be6kvyuxuzgg0e"}, Format: "table", DatasourceID: 34, TimeColumns: []string{"time"}}}}

	valuesArray := sendRequest(grafanaURL, serviceToken, costReq, "Cost Data")
	if len(valuesArray) < 2 {
		return make(map[string]float64), nil // Return empty map, not an error
	}

	result := make(map[string]float64)
	for i := 0; i < len(valuesArray[0]); i++ {
		result[strings.ToLower(toString(valuesArray[0][i]))] = toFloat64(valuesArray[1][i])
	}

	return result, nil
}

// Join data from all sources
func performLeftJoin(results *QueryResults) []JoinedRecord {
	var records []JoinedRecord

	for salesforceID, tenantData := range results.TenantData {
		record := JoinedRecord{
			SalesforceID:      salesforceID,
			Domain:            tenantData[0],
			TenantCreatedDate: tenantData[1],
			CreatedByEmail:    tenantData[2],
		}

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

// Activity: Save JSON report to file
func SaveReportActivity(ctx context.Context, records []JoinedRecord, filename string) error {
	// Calculate summary statistics
	summary := ReportSummary{
		TotalAccounts:  len(records),
		AccountsByType: make(map[string]int),
	}

	var totalCost float64
	var accountsWithCost int

	// Convert records and calculate summary
	var accounts []ChurnedAccount
	for _, record := range records {
		// Count by type
		accountType := record.SalesforceType
		if accountType == "" {
			accountType = "Unknown"
		}
		summary.AccountsByType[accountType]++

		// Sum costs
		if record.Cost > 0 {
			totalCost += record.Cost
			accountsWithCost++
		}

		// Convert to output format
		accounts = append(accounts, ChurnedAccount{
			Name:         getStringOrNull(record.SalesforceName),
			Type:         getStringOrNull(record.SalesforceType),
			Domain:       getStringOrNull(record.Domain),
			CreationDate: getStringOrNull(record.TenantCreatedDate),
			CreatedBy:    getStringOrNull(record.CreatedByEmail),
			CSM:          getStringOrNull(record.CSM),
			Cost:         getFloatOrNull(record.Cost),
		})
	}

	summary.TotalMonthlyCost = roundToTwoDecimals(totalCost)
	if accountsWithCost > 0 {
		summary.AverageCostPerTenant = roundToTwoDecimals(totalCost / float64(accountsWithCost))
	}

	// Create and save report
	report := ChurnReport{
		Metadata: ReportMetadata{
			ReportType:  "salesforce_churn_analysis",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			GeneratedBy: "live-churn-alert-workflow",
		},
		Summary:         summary,
		ChurnedAccounts: accounts,
	}

	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	return os.WriteFile(filename, jsonData, 0644)
}

// Helper: Send request to Grafana API (used by Activities)
func sendRequest(grafanaURL, serviceToken string, queryReq QueryRequest, requestName string) [][]interface{} {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	jsonData, _ := json.Marshal(queryReq)
	sugar.Infof("Executing %s", requestName)

	req, _ := http.NewRequestWithContext(context.Background(), "POST", grafanaURL+"/api/ds/query", bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", "Bearer "+serviceToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		sugar.Errorf("Error executing %s: %v", requestName, err)
		return nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		sugar.Errorf("%s failed: %s", requestName, resp.Status)
		return nil
	}

	var queryResp QueryResponse
	if err := json.Unmarshal(body, &queryResp); err != nil {
		sugar.Errorf("Error parsing %s response: %v", requestName, err)
		return nil
	}

	refIDMap := map[string]string{"Account Data": "A", "CSM Data": "C", "Tenant Data": "B", "Cost Data": "D"}
	refID := refIDMap[requestName]

	if result, exists := queryResp.Results[refID]; exists && len(result.Frames) > 0 {
		valuesArray := result.Frames[0].Data.Values
		sugar.Infof("Completed %s: %d rows", requestName, len(valuesArray[0]))
		return valuesArray
	}

	sugar.Infof("No data found in %s", requestName)
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

func getStringOrNull(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func getFloatOrNull(value float64) *float64 {
	if value == 0.0 {
		return nil
	}
	rounded := roundToTwoDecimals(value)
	return &rounded
}

func roundToTwoDecimals(value float64) float64 {
	return float64(int(value*100+0.5)) / 100
}

func formatTimestamp(timestampStr string) string {
	if timestampStr == "" {
		return "N/A"
	}

	if timestamp, err := strconv.ParseFloat(timestampStr, 64); err == nil {
		if int64(timestamp) > 1000000000000 {
			return time.Unix(int64(timestamp)/1000, 0).UTC().Format("2006-01-02")
		}
	}

	if len(timestampStr) >= 10 {
		return timestampStr[:10]
	}
	return timestampStr
}

// Synchronous wrappers for current main function (will be replaced by Temporal Workflow)
func fetchAccountDataSync(grafanaURL, serviceToken string) (map[string][]string, error) {
	return FetchAccountDataActivity(context.Background(), grafanaURL, serviceToken)
}

func fetchCSMDataSync(grafanaURL, serviceToken, inClause string) (map[string]string, error) {
	return FetchCSMDataActivity(context.Background(), grafanaURL, serviceToken, inClause)
}

func fetchTenantDataSync(grafanaURL, serviceToken, inClause string) (map[string][]string, error) {
	return FetchTenantDataActivity(context.Background(), grafanaURL, serviceToken, inClause)
}

func fetchCostDataSync(grafanaURL, serviceToken, inClause string) (map[string]float64, error) {
	return FetchCostDataActivity(context.Background(), grafanaURL, serviceToken, inClause)
}

func saveReportSync(records []JoinedRecord, filename string) error {
	return SaveReportActivity(context.Background(), records, filename)
}

// ============================================================================
// TEMPORAL WORKFLOW STRUCTURE (Future Implementation)
// ============================================================================
/*
// Workflow: Orchestrates the entire churn alert process
func SalesforceChurnWorkflow(ctx workflow.Context, config WorkflowConfig) (*ChurnReport, error) {
	// Configure retry policies for each activity
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Fetch account data (Activity)
	var accountData map[string][]string
	err := workflow.ExecuteActivity(ctx, FetchAccountDataActivity, config.GrafanaURL, config.ServiceToken).Get(ctx, &accountData)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch account data: %w", err)
	}

	// Step 2: Build IN clause (Workflow logic - deterministic)
	var salesforceIDs []string
	for id := range accountData {
		salesforceIDs = append(salesforceIDs, fmt.Sprintf("'%s'", id))
	}
	inClause := strings.Join(salesforceIDs, ",")

	// Step 3: Fetch additional data in parallel (Activities)
	var csmData map[string]string
	var tenantData map[string][]string
	var costData map[string]float64

	// Execute activities in parallel
	csmFuture := workflow.ExecuteActivity(ctx, FetchCSMDataActivity, config.GrafanaURL, config.ServiceToken, inClause)
	tenantFuture := workflow.ExecuteActivity(ctx, FetchTenantDataActivity, config.GrafanaURL, config.ServiceToken, inClause)
	costFuture := workflow.ExecuteActivity(ctx, FetchCostDataActivity, config.GrafanaURL, config.ServiceToken, inClause)

	// Wait for all activities to complete
	err = csmFuture.Get(ctx, &csmData)
	if err != nil {
		workflow.GetLogger(ctx).Warn("Failed to fetch CSM data", "error", err)
		csmData = make(map[string]string) // Continue with empty data
	}

	err = tenantFuture.Get(ctx, &tenantData)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tenant data: %w", err) // Critical data
	}

	err = costFuture.Get(ctx, &costData)
	if err != nil {
		workflow.GetLogger(ctx).Warn("Failed to fetch cost data", "error", err)
		costData = make(map[string]float64) // Continue with empty data
	}

	// Step 4: Perform data join (Workflow logic - deterministic)
	results := &QueryResults{
		AccountData: accountData,
		TenantData:  tenantData,
		CSMData:     csmData,
		CostData:    costData,
	}
	joinedRecords := performLeftJoin(results)

	// Step 5: Save report (Activity)
	filename := fmt.Sprintf("salesforce_report_%s.json", workflow.Now(ctx).Format("20060102_150405"))
	err = workflow.ExecuteActivity(ctx, SaveReportActivity, joinedRecords, filename).Get(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to save report: %w", err)
	}

	// Step 6: Generate final report structure (Workflow logic - deterministic)
	report := generateFinalReport(joinedRecords)
	return report, nil
}

// Worker registration function
func RegisterWorkflowsAndActivities(w worker.Worker) {
	// Register the workflow
	w.RegisterWorkflow(SalesforceChurnWorkflow)

	// Register all activities
	w.RegisterActivity(FetchAccountDataActivity)
	w.RegisterActivity(FetchCSMDataActivity)
	w.RegisterActivity(FetchTenantDataActivity)
	w.RegisterActivity(FetchCostDataActivity)
	w.RegisterActivity(SaveReportActivity)
}

// Configuration for the workflow
type WorkflowConfig struct {
	GrafanaURL   string
	ServiceToken string
}
*/
