package activities

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

	"go.uber.org/zap"

	"live-churn-alerting/internal/types"
)

// FetchAccountDataActivity fetches churned Salesforce account data
func FetchAccountDataActivity(ctx context.Context, grafanaURL, serviceToken string) (map[string][]string, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	accountQuery := `SELECT LOWER("ID") AS "salesforce_id", "NAME" AS "salesforce_name", "TYPE" AS "salesforce_type"
FROM "SALESFORCE"."ACCOUNT"
WHERE "TYPE" IN ('Lost Opportunity', 'Churned - Customer', 'Churned - Trial');`

	queryReq := types.QueryRequest{
		Queries: []types.Query{
			{
				QueryText: accountQuery,
				RefID:     "A",
				Datasource: types.DataSource{
					Type: "michelin-snowflake-datasource",
					UID:  "ae62hcbfe169sf",
				},
				DatasourceID: 40,
				Format:       "table",
			},
		},
	}

	valuesArray := sendRequest(grafanaURL, serviceToken, queryReq, "Account Data")
	if len(valuesArray) < 3 {
		return nil, fmt.Errorf("insufficient account data received")
	}

	accountData := make(map[string][]string)
	for i := 0; i < len(valuesArray[0]); i++ {
		salesforceID := types.ToString(valuesArray[0][i])
		salesforceName := types.ToString(valuesArray[1][i])
		salesforceType := types.ToString(valuesArray[2][i])
		if salesforceID != "" {
			accountData[salesforceID] = []string{salesforceName, salesforceType}
		}
	}

	sugar.Infof("Fetched %d account records", len(accountData))
	return accountData, nil
}

// FetchCSMDataActivity fetches CSM information for given Salesforce IDs
func FetchCSMDataActivity(ctx context.Context, grafanaURL, serviceToken, inClause string) (map[string]string, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	csmQuery := fmt.Sprintf(`SELECT DISTINCT LOWER("ID") AS "salesforce_id", "VITALLY_CSM_C" AS "csm"
FROM "SALESFORCE"."ACCOUNT"
WHERE LOWER("ID") IN (%s);`, inClause)

	queryReq := types.QueryRequest{
		Queries: []types.Query{
			{
				QueryText: csmQuery,
				RefID:     "C",
				Datasource: types.DataSource{
					Type: "michelin-snowflake-datasource",
					UID:  "ae62hcbfe169sf",
				},
				DatasourceID: 40,
				Format:       "table",
			},
		},
	}

	valuesArray := sendRequest(grafanaURL, serviceToken, queryReq, "CSM Data")
	if len(valuesArray) < 2 {
		sugar.Warn("No CSM data received")
		return make(map[string]string), nil
	}

	csmData := make(map[string]string)
	for i := 0; i < len(valuesArray[0]); i++ {
		salesforceID := types.ToString(valuesArray[0][i])
		csm := types.ToString(valuesArray[1][i])
		if salesforceID != "" {
			csmData[salesforceID] = csm
		}
	}

	sugar.Infof("Fetched CSM data for %d records", len(csmData))
	return csmData, nil
}

// FetchTenantDataActivity fetches tenant information for given Salesforce IDs
func FetchTenantDataActivity(ctx context.Context, grafanaURL, serviceToken, inClause string) (map[string][]string, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	// Convert IN clause format for PostgreSQL
	csvFormat := strings.ReplaceAll(strings.ReplaceAll(inClause, "'", ""), " ", "")

	tenantQuery := fmt.Sprintf(`SELECT DISTINCT
    t.domain,
    LOWER(a.salesforce_account_id) AS salesforce_id,
    t.created_at AS tenant_created_date,
    ue_created.email AS created_by_email
FROM tenants t
JOIN accounts a ON t.account_id = a.id
LEFT JOIN user_entity ue_created ON ue_created.id = t.created_by::varchar
WHERE LOWER(a.salesforce_account_id) = ANY(
    SELECT unnest(string_to_array('%s', ','))
) AND
t.status = 'LIVE';`, csvFormat)

	queryReq := types.QueryRequest{
		Queries: []types.Query{
			{
				RawSQL: tenantQuery,
				RefID:  "B",
				Datasource: types.DataSource{
					Type: "grafana-postgresql-datasource",
					UID:  "ceb9ek6fvijuoc",
				},
				DatasourceID: 36,
				Format:       "table",
			},
		},
	}

	valuesArray := sendRequest(grafanaURL, serviceToken, queryReq, "Tenant Data")
	if len(valuesArray) < 4 {
		sugar.Warn("No tenant data received")
		return make(map[string][]string), nil
	}

	tenantData := make(map[string][]string)
	for i := 0; i < len(valuesArray[0]); i++ {
		domain := types.ToString(valuesArray[0][i])
		salesforceID := types.ToString(valuesArray[1][i])
		createdAt := types.ToString(valuesArray[2][i])
		createdByEmail := types.ToString(valuesArray[3][i])

		// Format timestamp
		formattedDate := types.FormatTimestamp(createdAt)

		if salesforceID != "" {
			tenantData[salesforceID] = []string{domain, formattedDate, createdByEmail}
		}
	}

	sugar.Infof("Fetched tenant data for %d records", len(tenantData))
	return tenantData, nil
}

// FetchCostDataActivity fetches cost data for given Salesforce IDs
func FetchCostDataActivity(ctx context.Context, grafanaURL, serviceToken, inClause string) (map[string]float64, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	costQuery := fmt.Sprintf(`SELECT SALESFORCE_ID, SUM(TENANTCOST) AS COST
FROM "ATLAN_CLOUD"."PUBLIC"."MONTHLY_DASHBOARD_CUSTOMER_TENANT_COST_SALESFORCE_VIEW"
WHERE DATE_TRUNC('MONTH', BILLING_DATE) = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE))
AND LOWER("SALESFORCE_ID") IN (%s)
GROUP BY SALESFORCE_ID;`, inClause)

	queryReq := types.QueryRequest{
		Queries: []types.Query{
			{
				QueryText: costQuery,
				RefID:     "D",
				Datasource: types.DataSource{
					Type: "michelin-snowflake-datasource",
					UID:  "be6kvyuxuzgg0e",
				},
				DatasourceID: 34,
				Format:       "table",
			},
		},
	}

	valuesArray := sendRequest(grafanaURL, serviceToken, queryReq, "Cost Data")
	if len(valuesArray) < 2 {
		sugar.Warn("No cost data received")
		return make(map[string]float64), nil
	}

	costData := make(map[string]float64)
	for i := 0; i < len(valuesArray[0]); i++ {
		salesforceID := strings.ToLower(types.ToString(valuesArray[0][i]))
		costStr := types.ToString(valuesArray[1][i])
		if cost, err := strconv.ParseFloat(costStr, 64); err == nil && salesforceID != "" {
			costData[salesforceID] = cost
		}
	}

	sugar.Infof("Fetched cost data for %d records", len(costData))
	return costData, nil
}

// SaveReportActivity saves report to file
func SaveReportActivity(ctx context.Context, joinedRecords []types.JoinedRecord, filename string) error {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	var accounts []types.ChurnedAccount
	summary := types.ReportSummary{
		TotalAccounts:    len(joinedRecords),
		AccountsByType:   make(map[string]int),
		TotalMonthlyCost: 0,
	}

	var totalCost float64
	var accountsWithCost int

	for _, record := range joinedRecords {
		// Update summary statistics
		if record.SalesforceType != "" {
			summary.AccountsByType[record.SalesforceType]++
		}
		if record.Cost > 0 {
			totalCost += record.Cost
			accountsWithCost++
		}

		// Convert to output format
		accounts = append(accounts, types.ChurnedAccount{
			Name:         types.GetStringOrNull(record.SalesforceName),
			Type:         types.GetStringOrNull(record.SalesforceType),
			Domain:       types.GetStringOrNull(record.Domain),
			CreationDate: types.GetStringOrNull(record.TenantCreatedDate),
			CreatedBy:    types.GetStringOrNull(record.CreatedByEmail),
			CSM:          types.GetStringOrNull(record.CSM),
			Cost:         types.GetFloatOrNull(record.Cost),
		})
	}

	summary.TotalMonthlyCost = types.RoundToTwoDecimals(totalCost)
	if accountsWithCost > 0 {
		summary.AverageCostPerTenant = types.RoundToTwoDecimals(totalCost / float64(accountsWithCost))
	}

	report := types.ChurnReport{
		Metadata:        types.ReportMetadata{GeneratedAt: time.Now().Format(time.RFC3339)},
		Summary:         summary,
		ChurnedAccounts: accounts,
	}

	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	if err := os.WriteFile(filename, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write report file: %w", err)
	}

	sugar.Infof("Successfully saved report to %s", filename)
	return nil
}

// sendRequest sends request to Grafana API (used by Activities)
func sendRequest(grafanaURL, serviceToken string, queryReq types.QueryRequest, requestName string) [][]interface{} {
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

	var queryResp types.QueryResponse
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
