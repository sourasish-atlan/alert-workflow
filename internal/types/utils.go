package types

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Data processing functions
func PerformLeftJoin(results *QueryResults) []JoinedRecord {
	var joinedRecords []JoinedRecord

	// Left join from tenant data (preserve all tenant records)
	for salesforceID, tenantInfo := range results.TenantData {
		record := JoinedRecord{
			SalesforceID:      salesforceID,
			Domain:            tenantInfo[0],
			TenantCreatedDate: tenantInfo[1],
			CreatedByEmail:    tenantInfo[2],
		}

		// Join with account data
		if accountInfo, exists := results.AccountData[salesforceID]; exists {
			record.SalesforceName = accountInfo[0]
			record.SalesforceType = accountInfo[1]
		}

		// Join with CSM data
		if csm, exists := results.CSMData[salesforceID]; exists {
			record.CSM = csm
		}

		// Join with cost data
		if cost, exists := results.CostData[salesforceID]; exists {
			record.Cost = cost
		}

		joinedRecords = append(joinedRecords, record)
	}

	return joinedRecords
}

// Utility functions
func ToString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func FormatTimestamp(timestampStr string) string {
	if timestampStr == "" {
		return ""
	}

	// Handle scientific notation (e.g., "1.724648548232e+12")
	if strings.Contains(timestampStr, "e+") {
		if floatVal, err := strconv.ParseFloat(timestampStr, 64); err == nil {
			timestampStr = fmt.Sprintf("%.0f", floatVal)
		}
	}

	// Try parsing as Unix timestamp (seconds)
	if timestamp, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
		// Check if it's in milliseconds (13 digits) or seconds (10 digits)
		if timestamp > 9999999999 { // More than 10 digits, likely milliseconds
			timestamp = timestamp / 1000
		}
		return time.Unix(timestamp, 0).Format("2006-01-02")
	}

	// Try parsing as RFC3339 or other common formats
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timestampStr); err == nil {
			return t.Format("2006-01-02")
		}
	}

	return timestampStr // Return as-is if parsing fails
}

func GetStringOrNull(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func GetFloatOrNull(value float64) *float64 {
	if value == 0 {
		return nil
	}
	rounded := RoundToTwoDecimals(value)
	return &rounded
}

func RoundToTwoDecimals(value float64) float64 {
	return math.Round(value*100) / 100
}
