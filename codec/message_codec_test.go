package codec

import (
	"testing"
)

func TestStandardMessageCodec(t *testing.T) {
	// Create a standard message codec
	codec := NewStandardMessageCodec()

	// Create a test message
	msg := &Message{
		Topic: "test-topic",
		Flag:  0,
		Properties: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
		Body: []byte("test message body"),
	}

	// Test encoding
	data, err := codec.EncodeStandardMessage(msg, 10, "test remark")
	if err != nil {
		t.Errorf("Failed to encode message: %v", err)
	}
	if len(data) == 0 {
		t.Errorf("Expected encoded data to be non-empty")
	}

	// Test decoding
	decodedMsg, requestCode, remark, err := codec.DecodeStandardMessage(data)
	if err != nil {
		t.Errorf("Failed to decode message: %v", err)
	}
	if decodedMsg == nil {
		t.Errorf("Expected decoded message to be non-nil")
	}
	if requestCode != 10 {
		t.Errorf("Expected request code 10, got %d", requestCode)
	}
	if remark != "test remark" {
		t.Errorf("Expected remark 'test remark', got '%s'", remark)
	}
	if decodedMsg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", decodedMsg.Topic)
	}
	if string(decodedMsg.Body) != "test message body" {
		t.Errorf("Expected body 'test message body', got '%s'", string(decodedMsg.Body))
	}

	t.Logf("Standard message codec test completed")
}

func TestMessageValidation(t *testing.T) {
	// Create a standard message codec
	codec := NewStandardMessageCodec()

	// Test validation with valid message
	validMsg := &Message{
		Topic: "test-topic",
		Body:  []byte("test message body"),
	}
	err := codec.ValidateMessage(validMsg)
	if err != nil {
		t.Errorf("Expected valid message to pass validation, got error: %v", err)
	}

	// Test validation with invalid message (empty topic)
	invalidMsg := &Message{
		Topic: "",
		Body:  []byte("test message body"),
	}
	err = codec.ValidateMessage(invalidMsg)
	if err == nil {
		t.Errorf("Expected invalid message to fail validation")
	}

	t.Logf("Message validation test completed")
}
