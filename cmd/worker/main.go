package main

import (
	"os"

	"github.com/joho/godotenv"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"

	"live-churn-alerting/internal/activities"
	"live-churn-alerting/internal/shared"
	"live-churn-alerting/internal/workflows"
)

func main() {
	// Setup zap logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	// Load environment variables
	if err := godotenv.Load(); err != nil {
		sugar.Info("No .env file found, using environment variables")
	}

	// Get Temporal server address (default to localhost for development)
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

	// Create worker
	w := worker.New(temporalClient, shared.TaskQueue, worker.Options{})

	// Register workflows and activities
	registerWorkflowsAndActivities(w)

	sugar.Info("Starting Temporal worker", "taskQueue", shared.TaskQueue)

	// Start worker
	err = w.Run(worker.InterruptCh())
	if err != nil {
		sugar.Fatalf("Unable to start worker: %v", err)
	}
}

// registerWorkflowsAndActivities registers all workflows and activities with the worker
func registerWorkflowsAndActivities(w worker.Worker) {
	// Register the workflow
	w.RegisterWorkflow(workflows.LiveChurnAlertWorkflow)

	// Register all activities
	w.RegisterActivity(activities.FetchAccountDataActivity)
	w.RegisterActivity(activities.FetchCSMDataActivity)
	w.RegisterActivity(activities.FetchTenantDataActivity)
	w.RegisterActivity(activities.FetchCostDataActivity)
	w.RegisterActivity(activities.SaveReportActivity)
}
