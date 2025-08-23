package workflows

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"live-churn-alerting/internal/activities"
	"live-churn-alerting/internal/types"
)

// WorkflowConfig holds configuration for the Live churn alert workflow
type WorkflowConfig struct {
	GrafanaURL   string
	ServiceToken string
}

// ChurnReport is exported for use by client
type ChurnReport = types.ChurnReport

// LiveChurnAlertWorkflow orchestrates the entire churn alert process
func LiveChurnAlertWorkflow(ctx workflow.Context, config WorkflowConfig) (*ChurnReport, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Live churn alert workflow")

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
	err := workflow.ExecuteActivity(ctx, activities.FetchAccountDataActivity, config.GrafanaURL, config.ServiceToken).Get(ctx, &accountData)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch account data: %w", err)
	}

	if len(accountData) == 0 {
		logger.Info("No churned accounts found")
		return &ChurnReport{
			Metadata: types.ReportMetadata{GeneratedAt: workflow.Now(ctx).Format(time.RFC3339)},
			Summary: types.ReportSummary{
				TotalAccounts:        0,
				AccountsByType:       make(map[string]int),
				TotalMonthlyCost:     0,
				AverageCostPerTenant: 0,
			},
			ChurnedAccounts: []types.ChurnedAccount{},
		}, nil
	}

	// Step 2: Build IN clause (Workflow logic - deterministic)
	var salesforceIDs []string
	for id := range accountData {
		salesforceIDs = append(salesforceIDs, fmt.Sprintf("'%s'", id))
	}
	inClause := strings.Join(salesforceIDs, ",")
	logger.Info("Found churned accounts", "count", len(salesforceIDs))

	// Step 3: Fetch additional data in parallel (Activities)
	var csmData map[string]string
	var tenantData map[string][]string
	var costData map[string]float64

	// Execute activities in parallel
	csmFuture := workflow.ExecuteActivity(ctx, activities.FetchCSMDataActivity, config.GrafanaURL, config.ServiceToken, inClause)
	tenantFuture := workflow.ExecuteActivity(ctx, activities.FetchTenantDataActivity, config.GrafanaURL, config.ServiceToken, inClause)
	costFuture := workflow.ExecuteActivity(ctx, activities.FetchCostDataActivity, config.GrafanaURL, config.ServiceToken, inClause)

	// Wait for all activities to complete
	err = csmFuture.Get(ctx, &csmData)
	if err != nil {
		logger.Warn("Failed to fetch CSM data", "error", err)
		csmData = make(map[string]string) // Continue with empty data
	}

	err = tenantFuture.Get(ctx, &tenantData)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tenant data: %w", err) // Critical data
	}

	err = costFuture.Get(ctx, &costData)
	if err != nil {
		logger.Warn("Failed to fetch cost data", "error", err)
		costData = make(map[string]float64) // Continue with empty data
	}

	// Step 4: Perform data join (Workflow logic - deterministic)
	results := &types.QueryResults{
		AccountData: accountData,
		TenantData:  tenantData,
		CSMData:     csmData,
		CostData:    costData,
	}
	joinedRecords := types.PerformLeftJoin(results)

	if len(joinedRecords) == 0 {
		logger.Info("No records after join operation")
		return &ChurnReport{
			Metadata: types.ReportMetadata{GeneratedAt: workflow.Now(ctx).Format(time.RFC3339)},
			Summary: types.ReportSummary{
				TotalAccounts:        0,
				AccountsByType:       make(map[string]int),
				TotalMonthlyCost:     0,
				AverageCostPerTenant: 0,
			},
			ChurnedAccounts: []types.ChurnedAccount{},
		}, nil
	}

	// Step 5: Save report (Activity)
	filename := fmt.Sprintf("report_%s.json", workflow.Now(ctx).Format("20060102_150405"))
	err = workflow.ExecuteActivity(ctx, activities.SaveReportActivity, joinedRecords, filename).Get(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to save report: %w", err)
	}

	// Step 6: Generate final report structure (Workflow logic - deterministic)
	report := generateFinalReport(joinedRecords, workflow.Now(ctx))
	logger.Info("Workflow completed successfully", "records", len(joinedRecords), "filename", filename)

	return report, nil
}

// generateFinalReport generates final report (deterministic workflow logic)
func generateFinalReport(joinedRecords []types.JoinedRecord, timestamp time.Time) *ChurnReport {
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

	return &ChurnReport{
		Metadata:        types.ReportMetadata{GeneratedAt: timestamp.Format(time.RFC3339)},
		Summary:         summary,
		ChurnedAccounts: accounts,
	}
}
