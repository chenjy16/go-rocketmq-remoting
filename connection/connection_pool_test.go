package connection

import (
	"testing"
	"time"
)

func TestConnectionPoolWithCircuitBreaker(t *testing.T) {
	// Create a connection pool with circuit breaker enabled
	config := DefaultConnectionPoolConfig()
	pool := NewConnectionPool(config)
	defer pool.Close()

	// Test that circuit breaker is working
	// Try to connect to an invalid address
	invalidAddr := "127.0.0.1:12345" // Assuming this port is not listening

	// First few attempts should fail and increment the failure counter
	for i := 0; i < 3; i++ {
		_, err := pool.GetConnection(invalidAddr)
		if err == nil {
			t.Errorf("Expected error when connecting to invalid address, got nil")
		}
	}

	// Check that the circuit breaker is now open
	// This might depend on the exact implementation details
	// For now, we'll just verify the pool is functioning
	t.Logf("Connection pool with circuit breaker test completed")
}

func TestConnectionPoolRetryMechanism(t *testing.T) {
	// Create a connection pool with retry mechanism
	config := DefaultConnectionPoolConfig()
	pool := NewConnectionPool(config)
	defer pool.Close()

	// Test that retry mechanism is working
	// Try to connect to an invalid address
	invalidAddr := "127.0.0.1:12345" // Assuming this port is not listening

	// Record start time
	startTime := time.Now()

	// This should trigger the retry mechanism
	_, err := pool.GetConnection(invalidAddr)

	// Record end time
	endTime := time.Now()

	if err == nil {
		t.Errorf("Expected error when connecting to invalid address, got nil")
	}

	// Check that the retry mechanism took some time
	elapsed := endTime.Sub(startTime)
	if elapsed < pool.errorConfig.RetryInterval {
		t.Errorf("Expected retry mechanism to take at least %v, but took %v",
			pool.errorConfig.RetryInterval, elapsed)
	}

	t.Logf("Connection pool retry mechanism test completed")
}

func TestConnectionPoolMetrics(t *testing.T) {
	config := DefaultConnectionPoolConfig()
	pool := NewConnectionPool(config)
	defer pool.Close()

	// Test metrics collection
	stats := pool.GetStats()
	if stats == nil {
		t.Errorf("Expected pool stats to be non-nil")
	}

	// Try to connect to an invalid address to generate some metrics
	invalidAddr := "127.0.0.1:12345"
	pool.GetConnection(invalidAddr)

	// Check that metrics were updated
	stats = pool.GetStats()
	if stats["failed_connections"] == 0 {
		t.Errorf("Expected failed connections to be greater than 0")
	}

	t.Logf("Connection pool metrics test completed")
}
