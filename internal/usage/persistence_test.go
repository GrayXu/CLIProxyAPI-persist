package usage

import (
	"path/filepath"
	"testing"
	"time"

	appconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestBuildPersistenceConfigDefaultsToAuthDirSQLite(t *testing.T) {
	t.Parallel()

	cfg := &appconfig.Config{
		AuthDir:                "/tmp/auths",
		UsageStatisticsEnabled: true,
		UsageStatisticsStorage: appconfig.UsageStatisticsStorageConfig{
			Enabled: true,
		},
	}

	got := BuildPersistenceConfig(cfg)
	if !got.Enabled {
		t.Fatalf("expected persistence to be enabled")
	}
	if got.Driver != "sqlite" {
		t.Fatalf("expected sqlite driver, got %q", got.Driver)
	}
	wantPath := filepath.Join(cfg.AuthDir, "usage-statistics.db")
	if got.Path != wantPath {
		t.Fatalf("expected path %q, got %q", wantPath, got.Path)
	}
	if got.FlushInterval != 5*time.Second {
		t.Fatalf("expected 5s flush interval, got %s", got.FlushInterval)
	}
	if got.BatchSize != 128 {
		t.Fatalf("expected batch size 128, got %d", got.BatchSize)
	}
}

func TestSQLiteUsageStoreReloadsPersistedRecords(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "usage.db")
	store, err := openSQLiteUsageStore(dbPath)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	ts1 := time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)
	ts2 := ts1.Add(2 * time.Minute)
	if err := store.insertBatch([]persistedUsageRecord{
		{
			APIName:   "key-1",
			ModelName: "gpt-5",
			Detail: RequestDetail{
				Timestamp: ts1,
				Source:    "chat",
				AuthIndex: "0",
				Tokens: TokenStats{
					InputTokens:  10,
					OutputTokens: 5,
					TotalTokens:  15,
				},
			},
		},
		{
			APIName:   "key-1",
			ModelName: "gpt-5",
			Detail: RequestDetail{
				Timestamp: ts2,
				Source:    "chat",
				AuthIndex: "0",
				Failed:    true,
				Tokens: TokenStats{
					InputTokens:  4,
					OutputTokens: 1,
					TotalTokens:  5,
				},
			},
		},
	}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	stats := NewRequestStatistics()
	if err := store.loadInto(stats); err != nil {
		t.Fatalf("load into stats: %v", err)
	}
	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 2 {
		t.Fatalf("expected 2 total requests, got %d", snapshot.TotalRequests)
	}
	if snapshot.SuccessCount != 1 {
		t.Fatalf("expected 1 success, got %d", snapshot.SuccessCount)
	}
	if snapshot.FailureCount != 1 {
		t.Fatalf("expected 1 failure, got %d", snapshot.FailureCount)
	}
	if snapshot.TotalTokens != 20 {
		t.Fatalf("expected 20 total tokens, got %d", snapshot.TotalTokens)
	}
	apiSnapshot, ok := snapshot.APIs["key-1"]
	if !ok {
		t.Fatalf("expected api snapshot for key-1")
	}
	modelSnapshot, ok := apiSnapshot.Models["gpt-5"]
	if !ok {
		t.Fatalf("expected model snapshot for gpt-5")
	}
	if len(modelSnapshot.Details) != 2 {
		t.Fatalf("expected 2 request details, got %d", len(modelSnapshot.Details))
	}
}

func TestSQLiteUsageStoreInsertSnapshotDeduplicates(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "usage.db")
	store, err := openSQLiteUsageStore(dbPath)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	snapshot := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"key-1": {
				Models: map[string]ModelSnapshot{
					"gpt-5": {
						Details: []RequestDetail{
							{
								Timestamp: time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC),
								Source:    "chat",
								AuthIndex: "1",
								Tokens: TokenStats{
									InputTokens:  3,
									OutputTokens: 2,
									TotalTokens:  5,
								},
							},
						},
					},
				},
			},
		},
	}

	first, err := store.insertSnapshot(snapshot)
	if err != nil {
		t.Fatalf("first insert snapshot: %v", err)
	}
	if first.Added != 1 || first.Skipped != 0 {
		t.Fatalf("unexpected first result: %+v", first)
	}

	second, err := store.insertSnapshot(snapshot)
	if err != nil {
		t.Fatalf("second insert snapshot: %v", err)
	}
	if second.Added != 0 || second.Skipped != 1 {
		t.Fatalf("unexpected second result: %+v", second)
	}
}
