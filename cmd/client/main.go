package main

import (
	"context"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"live-churn-alerting/internal/shared"
	"live-churn-alerting/internal/workflows"
)

// StartWorkflow starts the Live churn alert workflow
func StartWorkflow() error {
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
		sugar.Fatal("GRAFANA_URL environment variable is required")
	}

	// Get Temporal server address
	temporalAddress := os.Getenv("TEMPORAL_ADDRESS")
	if temporalAddress == "" {
		temporalAddress = "localhost:7233"
	}

	// Create Temporal client
	clientOptions := client.Options{
		HostPort: temporalAddress,
		Logger:   shared.NewZapAdapter(logger),
	}

	temporalClient, err := client.Dial(clientOptions)
	if err != nil {
		sugar.Fatalf("Unable to create Temporal client: %v", err)
	}
	defer temporalClient.Close()

	// Configure workflow options
	workflowOptions := client.StartWorkflowOptions{
		ID:        "live-churn-alert-workflow-" + time.Now().Format("20060102-150405"),
		TaskQueue: shared.TaskQueue,
	}

	// Prepare workflow configuration
	config := workflows.WorkflowConfig{
		GrafanaURL:   grafanaURL,
		ServiceToken: serviceToken,
	}

	sugar.Info("Starting Live churn alert workflow", "workflowID", workflowOptions.ID)

	// Start workflow
	workflowRun, err := temporalClient.ExecuteWorkflow(context.Background(), workflowOptions, workflows.LiveChurnAlertWorkflow, config)
	if err != nil {
		sugar.Errorf("Unable to execute workflow: %v", err)
		return err
	}

	sugar.Info("Workflow started", "workflowID", workflowRun.GetID(), "runID", workflowRun.GetRunID())

	// Wait for workflow completion
	var result workflows.ChurnReport
	err = workflowRun.Get(context.Background(), &result)
	if err != nil {
		sugar.Errorf("Unable to get workflow result: %v", err)
		return err
	}

	sugar.Info("Workflow completed successfully",
		"totalAccounts", result.Summary.TotalAccounts,
		"totalCost", result.Summary.TotalMonthlyCost,
		"generatedAt", result.Metadata.GeneratedAt)

	return nil
}

func main() {
	if err := StartWorkflow(); err != nil {
		os.Exit(1)
	}
}
