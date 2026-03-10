package usage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
	_ "modernc.org/sqlite"
)

const persistenceQueueSize = 4096

var persistenceEnabled atomic.Bool

type PersistenceConfig struct {
	Enabled       bool
	Driver        string
	Path          string
	FlushInterval time.Duration
	BatchSize     int
}

type PersistencePlugin struct{}

func NewPersistencePlugin() *PersistencePlugin { return &PersistencePlugin{} }

func (p *PersistencePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if p == nil || !StatisticsEnabled() || !persistenceEnabled.Load() {
		return
	}
	defaultPersistence.enqueue(normalisePersistedRecord(ctx, record))
}

func BuildPersistenceConfig(cfg *appconfig.Config) PersistenceConfig {
	if cfg == nil {
		return PersistenceConfig{}
	}
	storage := cfg.UsageStatisticsStorage
	driver := strings.ToLower(strings.TrimSpace(storage.Driver))
	if driver == "" {
		driver = appconfig.DefaultUsageStatsDriver
	}
	path := strings.TrimSpace(storage.Path)
	if path == "" && cfg.AuthDir != "" {
		path = filepath.Join(cfg.AuthDir, "usage-statistics.db")
	}
	flushSeconds := storage.FlushIntervalSeconds
	if flushSeconds <= 0 {
		flushSeconds = appconfig.DefaultUsageStatsFlushSecs
	}
	batchSize := storage.BatchSize
	if batchSize <= 0 {
		batchSize = appconfig.DefaultUsageStatsBatchSize
	}
	return PersistenceConfig{
		Enabled:       cfg.UsageStatisticsEnabled && storage.Enabled,
		Driver:        driver,
		Path:          path,
		FlushInterval: time.Duration(flushSeconds) * time.Second,
		BatchSize:     batchSize,
	}
}

func ConfigurePersistence(cfg PersistenceConfig, loadExisting bool) error {
	defaultPersistence.start()
	if !cfg.Enabled {
		persistenceEnabled.Store(false)
		return defaultPersistence.disable()
	}
	normalised, err := normalisePersistenceConfig(cfg)
	if err != nil {
		return err
	}
	store, err := openSQLiteUsageStore(normalised.Path)
	if err != nil {
		return err
	}
	if loadExisting {
		if errLoad := store.loadInto(defaultRequestStatistics); errLoad != nil {
			_ = store.Close()
			return errLoad
		}
	}
	if errSwap := defaultPersistence.swap(normalised, store); errSwap != nil {
		_ = store.Close()
		return errSwap
	}
	persistenceEnabled.Store(true)
	return nil
}

func ShutdownPersistence(ctx context.Context) error {
	persistenceEnabled.Store(false)
	return defaultPersistence.shutdown(ctx)
}

func PersistSnapshot(snapshot StatisticsSnapshot) (MergeResult, error) {
	defaultPersistence.start()
	if !persistenceEnabled.Load() {
		return MergeResult{}, nil
	}
	return defaultPersistence.persistSnapshot(snapshot)
}

type persistenceCommandType int

const (
	persistenceCommandSwap persistenceCommandType = iota
	persistenceCommandDisable
	persistenceCommandPersistSnapshot
	persistenceCommandShutdown
)

type persistenceCommand struct {
	kind     persistenceCommandType
	cfg      PersistenceConfig
	store    *sqliteUsageStore
	snapshot StatisticsSnapshot
	reply    chan persistenceCommandResult
}

type persistenceCommandResult struct {
	result MergeResult
	err    error
}

type persistenceController struct {
	once     sync.Once
	queue    chan persistedUsageRecord
	commands chan persistenceCommand
}

var defaultPersistence persistenceController

func (c *persistenceController) start() {
	c.once.Do(func() {
		c.queue = make(chan persistedUsageRecord, persistenceQueueSize)
		c.commands = make(chan persistenceCommand)
		go c.run()
	})
}

func (c *persistenceController) enqueue(record persistedUsageRecord) {
	if c == nil {
		return
	}
	select {
	case c.queue <- record:
	default:
		log.Warn("usage persistence queue full; temporarily blocking until SQLite writer catches up")
		c.queue <- record
	}
}

func (c *persistenceController) swap(cfg PersistenceConfig, store *sqliteUsageStore) error {
	reply := make(chan persistenceCommandResult, 1)
	c.commands <- persistenceCommand{
		kind:  persistenceCommandSwap,
		cfg:   cfg,
		store: store,
		reply: reply,
	}
	return (<-reply).err
}

func (c *persistenceController) disable() error {
	reply := make(chan persistenceCommandResult, 1)
	c.commands <- persistenceCommand{
		kind:  persistenceCommandDisable,
		reply: reply,
	}
	return (<-reply).err
}

func (c *persistenceController) persistSnapshot(snapshot StatisticsSnapshot) (MergeResult, error) {
	reply := make(chan persistenceCommandResult, 1)
	c.commands <- persistenceCommand{
		kind:     persistenceCommandPersistSnapshot,
		snapshot: snapshot,
		reply:    reply,
	}
	result := <-reply
	return result.result, result.err
}

func (c *persistenceController) shutdown(ctx context.Context) error {
	if c == nil || c.commands == nil {
		return nil
	}
	reply := make(chan persistenceCommandResult, 1)
	select {
	case c.commands <- persistenceCommand{kind: persistenceCommandShutdown, reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case result := <-reply:
		return result.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *persistenceController) run() {
	cfg := PersistenceConfig{
		FlushInterval: time.Duration(appconfig.DefaultUsageStatsFlushSecs) * time.Second,
		BatchSize:     appconfig.DefaultUsageStatsBatchSize,
	}
	ticker := time.NewTicker(cfg.FlushInterval)
	defer ticker.Stop()

	var (
		store   *sqliteUsageStore
		batch   []persistedUsageRecord
		enabled bool
	)

	flush := func() error {
		if !enabled || store == nil || len(batch) == 0 {
			batch = batch[:0]
			return nil
		}
		if err := store.insertBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	resetTicker := func(interval time.Duration) {
		if interval <= 0 {
			interval = time.Duration(appconfig.DefaultUsageStatsFlushSecs) * time.Second
		}
		ticker.Reset(interval)
	}

	for {
		select {
		case record := <-c.queue:
			if !enabled || store == nil {
				continue
			}
			batch = append(batch, record)
			if len(batch) >= cfg.BatchSize {
				if err := flush(); err != nil {
					log.WithError(err).Error("usage persistence flush failed")
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				log.WithError(err).Error("usage persistence flush failed")
			}
		case command := <-c.commands:
			var result persistenceCommandResult
			switch command.kind {
			case persistenceCommandSwap:
				if err := flush(); err != nil {
					result.err = err
					break
				}
				if store != nil && store != command.store {
					if errClose := store.Close(); errClose != nil {
						log.WithError(errClose).Warn("failed to close previous usage SQLite store")
					}
				}
				cfg = command.cfg
				store = command.store
				enabled = command.cfg.Enabled && command.store != nil
				resetTicker(cfg.FlushInterval)
			case persistenceCommandDisable:
				if err := flush(); err != nil {
					result.err = err
					break
				}
				enabled = false
				if store != nil {
					if errClose := store.Close(); errClose != nil {
						result.err = errClose
						break
					}
				}
				store = nil
			case persistenceCommandPersistSnapshot:
				if err := flush(); err != nil {
					result.err = err
					break
				}
				if enabled && store != nil {
					result.result, result.err = store.insertSnapshot(command.snapshot)
				}
			case persistenceCommandShutdown:
				if err := flush(); err != nil {
					result.err = err
				}
				if store != nil {
					if errClose := store.Close(); errClose != nil && result.err == nil {
						result.err = errClose
					}
				}
				store = nil
				enabled = false
			}
			command.reply <- result
		}
	}
}

type sqliteUsageStore struct {
	db   *sql.DB
	path string
}

func openSQLiteUsageStore(path string) (*sqliteUsageStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("usage persistence path is required when storage is enabled")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create usage persistence directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open usage sqlite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	store := &sqliteUsageStore{db: db, path: path}
	if err = store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *sqliteUsageStore) init() error {
	if s == nil || s.db == nil {
		return errors.New("usage sqlite store is nil")
	}
	statements := []string{
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA synchronous = NORMAL;`,
		`CREATE TABLE IF NOT EXISTS usage_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			dedup_key TEXT NOT NULL UNIQUE,
			api_name TEXT NOT NULL,
			model_name TEXT NOT NULL,
			requested_at_unix_ns INTEGER NOT NULL,
			source TEXT NOT NULL DEFAULT '',
			auth_index TEXT NOT NULL DEFAULT '',
			failed INTEGER NOT NULL,
			input_tokens INTEGER NOT NULL,
			output_tokens INTEGER NOT NULL,
			reasoning_tokens INTEGER NOT NULL,
			cached_tokens INTEGER NOT NULL,
			total_tokens INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_usage_records_requested_at ON usage_records(requested_at_unix_ns);`,
		`CREATE INDEX IF NOT EXISTS idx_usage_records_api_model ON usage_records(api_name, model_name);`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("initialise usage sqlite database: %w", err)
		}
	}
	return nil
}

func (s *sqliteUsageStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *sqliteUsageStore) insertBatch(records []persistedUsageRecord) error {
	if s == nil || s.db == nil || len(records) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("begin usage sqlite transaction: %w", err)
	}
	stmt, err := tx.PrepareContext(context.Background(), `INSERT OR IGNORE INTO usage_records (
		dedup_key, api_name, model_name, requested_at_unix_ns, source, auth_index, failed,
		input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare usage sqlite insert: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	for _, record := range records {
		detail := record.Detail
		detail.Tokens = normaliseTokenStats(detail.Tokens)
		if _, errExec := stmt.ExecContext(
			context.Background(),
			dedupKey(record.APIName, record.ModelName, detail),
			record.APIName,
			record.ModelName,
			detail.Timestamp.UnixNano(),
			detail.Source,
			detail.AuthIndex,
			boolToInt(detail.Failed),
			detail.Tokens.InputTokens,
			detail.Tokens.OutputTokens,
			detail.Tokens.ReasoningTokens,
			detail.Tokens.CachedTokens,
			detail.Tokens.TotalTokens,
		); errExec != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert usage sqlite record: %w", errExec)
		}
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return fmt.Errorf("commit usage sqlite transaction: %w", errCommit)
	}
	return nil
}

func (s *sqliteUsageStore) insertSnapshot(snapshot StatisticsSnapshot) (MergeResult, error) {
	records := make([]persistedUsageRecord, 0)
	for apiName, apiSnapshot := range snapshot.APIs {
		apiName = strings.TrimSpace(apiName)
		if apiName == "" {
			continue
		}
		for modelName, modelSnapshot := range apiSnapshot.Models {
			modelName = strings.TrimSpace(modelName)
			if modelName == "" {
				modelName = "unknown"
			}
			for _, detail := range modelSnapshot.Details {
				detail.Tokens = normaliseTokenStats(detail.Tokens)
				if detail.Timestamp.IsZero() {
					detail.Timestamp = time.Now()
				}
				records = append(records, persistedUsageRecord{
					APIName:   apiName,
					ModelName: modelName,
					Detail:    detail,
				})
			}
		}
	}
	if len(records) == 0 {
		return MergeResult{}, nil
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return MergeResult{}, fmt.Errorf("begin usage sqlite import transaction: %w", err)
	}
	stmt, err := tx.PrepareContext(context.Background(), `INSERT OR IGNORE INTO usage_records (
		dedup_key, api_name, model_name, requested_at_unix_ns, source, auth_index, failed,
		input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return MergeResult{}, fmt.Errorf("prepare usage sqlite import: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	result := MergeResult{}
	for _, record := range records {
		execResult, errExec := stmt.ExecContext(
			context.Background(),
			dedupKey(record.APIName, record.ModelName, record.Detail),
			record.APIName,
			record.ModelName,
			record.Detail.Timestamp.UnixNano(),
			record.Detail.Source,
			record.Detail.AuthIndex,
			boolToInt(record.Detail.Failed),
			record.Detail.Tokens.InputTokens,
			record.Detail.Tokens.OutputTokens,
			record.Detail.Tokens.ReasoningTokens,
			record.Detail.Tokens.CachedTokens,
			record.Detail.Tokens.TotalTokens,
		)
		if errExec != nil {
			_ = tx.Rollback()
			return MergeResult{}, fmt.Errorf("insert usage sqlite import record: %w", errExec)
		}
		affected, errAffected := execResult.RowsAffected()
		if errAffected != nil {
			_ = tx.Rollback()
			return MergeResult{}, fmt.Errorf("read usage sqlite import result: %w", errAffected)
		}
		if affected > 0 {
			result.Added += affected
		} else {
			result.Skipped++
		}
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return MergeResult{}, fmt.Errorf("commit usage sqlite import transaction: %w", errCommit)
	}
	return result, nil
}

func (s *sqliteUsageStore) loadInto(stats *RequestStatistics) error {
	if s == nil || s.db == nil || stats == nil {
		return nil
	}
	rows, err := s.db.Query(`SELECT api_name, model_name, requested_at_unix_ns, source, auth_index, failed, input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens FROM usage_records ORDER BY id`)
	if err != nil {
		return fmt.Errorf("query usage sqlite records: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	stats.mu.Lock()
	defer stats.mu.Unlock()
	for rows.Next() {
		var (
			apiName         string
			modelName       string
			requestedAtUnix int64
			source          string
			authIndex       string
			failedInt       int
			detail          RequestDetail
		)
		if errScan := rows.Scan(
			&apiName,
			&modelName,
			&requestedAtUnix,
			&source,
			&authIndex,
			&failedInt,
			&detail.Tokens.InputTokens,
			&detail.Tokens.OutputTokens,
			&detail.Tokens.ReasoningTokens,
			&detail.Tokens.CachedTokens,
			&detail.Tokens.TotalTokens,
		); errScan != nil {
			return fmt.Errorf("scan usage sqlite record: %w", errScan)
		}
		apiName = strings.TrimSpace(apiName)
		if apiName == "" {
			continue
		}
		modelName = strings.TrimSpace(modelName)
		if modelName == "" {
			modelName = "unknown"
		}
		detail.Timestamp = time.Unix(0, requestedAtUnix)
		detail.Source = source
		detail.AuthIndex = authIndex
		detail.Failed = failedInt != 0
		detail.Tokens = normaliseTokenStats(detail.Tokens)

		apiStatsValue, ok := stats.apis[apiName]
		if !ok || apiStatsValue == nil {
			apiStatsValue = &apiStats{Models: make(map[string]*modelStats)}
			stats.apis[apiName] = apiStatsValue
		} else if apiStatsValue.Models == nil {
			apiStatsValue.Models = make(map[string]*modelStats)
		}
		stats.recordImported(apiName, modelName, apiStatsValue, detail)
	}
	if errRows := rows.Err(); errRows != nil {
		return fmt.Errorf("iterate usage sqlite records: %w", errRows)
	}
	return nil
}

func normalisePersistenceConfig(cfg PersistenceConfig) (PersistenceConfig, error) {
	cfg.Driver = strings.ToLower(strings.TrimSpace(cfg.Driver))
	if cfg.Driver == "" {
		cfg.Driver = appconfig.DefaultUsageStatsDriver
	}
	if cfg.Driver != "sqlite" {
		return PersistenceConfig{}, fmt.Errorf("unsupported usage statistics storage driver: %s", cfg.Driver)
	}
	cfg.Path = strings.TrimSpace(cfg.Path)
	if cfg.Path == "" {
		return PersistenceConfig{}, errors.New("usage statistics storage path is empty")
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Duration(appconfig.DefaultUsageStatsFlushSecs) * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = appconfig.DefaultUsageStatsBatchSize
	}
	cfg.Enabled = true
	return cfg, nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
