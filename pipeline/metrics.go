package pipeline

import (
	"context"
	"fmt"
	"log"
	"os"
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

// MetricsConfig holds the configuration for connecting to ClickHouse metrics database
type MetricsConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

// DefaultMetricsConfig returns the default configuration for metrics using environment variables
func DefaultMetricsConfig() MetricsConfig {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost"
	}
	port := 9000
	if portEnv := os.Getenv("CLICKHOUSE_PORT"); portEnv != "" {
		_, err := fmt.Sscanf(portEnv, "%d", &port)
		if err != nil {
			return MetricsConfig{}
		}
	}
	return MetricsConfig{
		Host:     host,
		Port:     port,
		Database: getEnvOrDefault("CLICKHOUSE_DB", "default"),
		Username: getEnvOrDefault("CLICKHOUSE_USER", "default"),
		Password: getEnvOrDefault("CLICKHOUSE_PASSWORD", "default"),
	}
}

// getEnvOrDefault returns the environment variable value or the default if not set
func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
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
                duration Float64,
                count UInt64,
                timestamp DateTime,
                duration_ns Int64,
                bytes_per_op Int64,
                allocs_per_op Int64
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
func logMetric(name string, duration time.Duration, count int, durationNs, bytesPerOp, allocsPerOp int64) {
	if !metricsEnabled || metricsConn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := metricsConn.Exec(ctx,
		`INSERT INTO metrics 
        (stage, duration, count, timestamp, duration_ns, bytes_per_op, allocs_per_op) 
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name,
		duration.Seconds(),
		count,
		time.Now(),
		durationNs,
		bytesPerOp,
		allocsPerOp,
	)

	if err != nil {
		log.Printf("Error inserting metric for stage '%s': %v", name, err)
	}
}

// MetricStage wraps a pipeline stage to record metrics about its execution
func MetricStage[I, O any](name string, inner Stage[I, O]) Stage[I, O] {
	return func(in <-chan I) <-chan O {
		start := time.Now()
		innerOut := inner(in)
		out := make(chan O, cap(innerOut))
		var count int
		var mu sync.Mutex
		go func() {
			defer close(out)
			for val := range innerOut {
				mu.Lock()
				count++
				mu.Unlock()
				out <- val
			}
			duration := time.Since(start)
			logMetric(name, duration, count, 0, 0, 0)
		}()
		return out
	}
}

// init initializes the metrics system when the package is loaded
func init() {
	config := DefaultMetricsConfig()
	log.Printf("Metrics config: %+v", config)
	if err := InitMetrics(config); err != nil {
		log.Printf("Failed to initialize ClickHouse metrics: %v", err)
	} else {
		log.Println("Metrics system initialized successfully")
	}
}
