package pipeline

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var (
	metricsConn    clickhouse.Conn
	metricsOnce    sync.Once
	metricsErr     error
	metricsEnabled bool
)

var (
	defaultHost = getEnvOrDefault("CLICKHOUSE_HOST", "localhost")
	defaultPort = getEnvAsIntOnly("CLICKHOUSE_PORT", 9000)
	defaultDB   = getEnvOrDefault("CLICKHOUSE_DB", "default")
	defaultUser = getEnvOrDefault("CLICKHOUSE_USER", "default")
	defaultPass = getEnvOrDefault("CLICKHOUSE_PASSWORD", "default")
)

// MetricsConfig holds the configuration for connecting to ClickHouse metrics database
type MetricsConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

var defaultMetricsConfig = MetricsConfig{
	Host:     defaultHost,
	Port:     defaultPort,
	Database: defaultDB,
	Username: defaultUser,
	Password: defaultPass,
}

// InitMetrics initializes the ClickHouse connection and creates the metrics table if needed
func InitMetrics(config MetricsConfig) error {
	metricsOnce.Do(func() {
		addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
		metricsConn, metricsErr = clickhouse.Open(&clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: config.Database,
				Username: config.Username,
				Password: config.Password,
			},
			MaxOpenConns:     50,
			MaxIdleConns:     10,
			ConnMaxLifetime:  time.Hour,
			DialTimeout:      30 * time.Second,
			ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		})
		if metricsErr != nil {
			log.Printf("Failed to connect to ClickHouse: %v", metricsErr)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		createTableSQL := `
            CREATE TABLE IF NOT EXISTS metrics (
                stage String,
                count Int64,
                duration Float64,
                memory Int64,
                timestamp DateTime
            ) ENGINE = MergeTree()
            ORDER BY (stage, count, timestamp)
        `
		metricsErr = metricsConn.Exec(ctx, createTableSQL)
		if metricsErr != nil {
			log.Printf("Failed to create metrics table: %v", metricsErr)
			return
		}

		metricsEnabled = true
		log.Println("Metrics system initialized successfully")
	})
	return metricsErr
}

// logMetric logs a metric entry to the ClickHouse database
func logMetric(name string, count int64, duration float64, memory int64) {
	if !metricsEnabled || metricsConn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := metricsConn.Exec(ctx,
		`INSERT INTO metrics 
        (stage, count, duration, memory, timestamp) 
        VALUES (?, ?, ?, ?, ?)`,
		name,
		count,
		duration,
		memory,
		time.Now(),
	)

	if err != nil {
		log.Printf("Error inserting metric for stage '%s': %v", name, err)
	}
}

// MetricStage wraps a pipeline stage to record metrics about its execution.
//
// Example:
//
//	stage := MetricStage("double-stage", MapStage[int](func(x int) int { return x * 2 }))
//	result := Collect(stage, []int{1, 2, 3})
func MetricStage[I, O any](name string, inner Stage[I, O]) Stage[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O, max(1, cap(in)))
		go func() {
			defer close(out)
			var memStart, memEnd runtime.MemStats
			runtime.ReadMemStats(&memStart)
			start := time.Now()
			count := int64(0)
			for v := range inner(in) {
				count++
				out <- v
			}
			runtime.ReadMemStats(&memEnd)
			duration := time.Since(start)
			if duration == 0 {
				return
			}
			logMetric(
				name,
				count,
				duration.Seconds(),
				int64(memEnd.HeapInuse-memStart.HeapInuse),
			)
		}()
		return out
	}
}

// init initializes the metrics system when the package is loaded
func init() {
	config := defaultMetricsConfig
	log.Printf("Initializing metrics with ClickHouse at %s:%d", config.Host, config.Port)
	if err := InitMetrics(config); err != nil {
		log.Printf("Failed to initialize ClickHouse metrics: %v", err)
	} else {
		log.Println("Metrics system initialized successfully")
	}
}
