package heartbeat

import (
	"testing"
	"time"
)

func TestHeartbeatStats(t *testing.T) {
	// Create a heartbeat manager
	manager := &HeartbeatManager{
		stats: &HeartbeatStats{},
	}

	// Test updating stats with success
	manager.updateStats(true, 10*time.Millisecond)

	// Check stats
	stats := manager.GetStats()
	if stats.SuccessCount != 1 {
		t.Errorf("Expected success count 1, got %d", stats.SuccessCount)
	}
	if stats.AvgResponseTime != 10*time.Millisecond {
		t.Errorf("Expected avg response time 10ms, got %v", stats.AvgResponseTime)
	}

	// Test updating stats with failure
	manager.updateStats(false, 5*time.Millisecond)

	// Check stats again
	stats = manager.GetStats()
	if stats.FailureCount != 1 {
		t.Errorf("Expected failure count 1, got %d", stats.FailureCount)
	}

	t.Logf("Heartbeat stats test completed")
}

func TestHeartbeatProcessorStats(t *testing.T) {
	// Create a heartbeat processor
	processor := NewHeartbeatProcessor()

	// Test updating stats
	processor.updateStats()

	// Check stats
	stats := processor.GetStats()
	if stats.TotalHeartbeats != 1 {
		t.Errorf("Expected total heartbeats 1, got %d", stats.TotalHeartbeats)
	}

	t.Logf("Heartbeat processor stats test completed")
}
