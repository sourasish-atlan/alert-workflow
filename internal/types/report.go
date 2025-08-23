package types

// Report structures
type ChurnReport struct {
	Metadata        ReportMetadata   `json:"metadata"`
	Summary         ReportSummary    `json:"summary"`
	ChurnedAccounts []ChurnedAccount `json:"churned_accounts"`
}

type ReportMetadata struct {
	GeneratedAt string `json:"generated_at"`
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
