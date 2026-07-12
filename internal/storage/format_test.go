package storage

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/model"
)

func TestFormat_RoundTripBasic(t *testing.T) {
	now := time.Now().UTC().UnixMilli()

	records := []model.StoredRecord{
		{
			Offset:    100,
			Timestamp: now - 10,
			Key:       []byte("k1"),
			Value:     []byte("v1"),
			Headers: []model.Header{
				{Key: "hk1", Value: []byte("hv1")},
				{Key: "hk2", Value: []byte("hv2")},
			},
		},
		{
			Offset:    101,
			Timestamp: now, // max timestamp
			Key:       nil,
			Value:     []byte(""), // empty but non-nil
			Headers:   nil,
		},
	}

	encoded, err := EncodeBatch(records)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	decoded, err := DecodeBatch(encoded)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if len(decoded) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(decoded))
	}

	// Record 1 validation
	r1 := decoded[0]
	if r1.Offset != 100 {
		t.Errorf("expected offset 100, got %d", r1.Offset)
	}
	if r1.Timestamp != now-10 {
		t.Errorf("expected timestamp %d, got %d", now-10, r1.Timestamp)
	}
	if !bytes.Equal(r1.Key, []byte("k1")) {
		t.Errorf("expected key 'k1', got %q", string(r1.Key))
	}
	if !bytes.Equal(r1.Value, []byte("v1")) {
		t.Errorf("expected value 'v1', got %q", string(r1.Value))
	}
	if len(r1.Headers) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(r1.Headers))
	}
	if r1.Headers[0].Key != "hk1" || !bytes.Equal(r1.Headers[0].Value, []byte("hv1")) {
		t.Errorf("header 1 mismatch: %v", r1.Headers[0])
	}

	// Record 2 validation
	r2 := decoded[1]
	if r2.Offset != 101 {
		t.Errorf("expected offset 101, got %d", r2.Offset)
	}
	if r2.Timestamp != now {
		t.Errorf("expected timestamp %d, got %d", now, r2.Timestamp)
	}
	if r2.Key != nil {
		t.Errorf("expected key to be nil, got %v", r2.Key)
	}
	if r2.Value == nil || len(r2.Value) != 0 {
		t.Errorf("expected empty non-nil value, got %v", r2.Value)
	}
}

func TestFormat_DecodeCorrupted(t *testing.T) {
	records := []model.StoredRecord{
		{
			Offset:    0,
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte("key"),
			Value:     []byte("val"),
		},
	}

	encoded, err := EncodeBatch(records)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// 1. Magic bytes corrupted
	corruptedMagic := make([]byte, len(encoded))
	copy(corruptedMagic, encoded)
	corruptedMagic[0] = 0x00
	_, err = DecodeBatch(corruptedMagic)
	if !errors.Is(err, ErrMalformedMagic) {
		t.Errorf("expected ErrMalformedMagic, got %v", err)
	}

	// 2. Unsupported version
	corruptedVersion := make([]byte, len(encoded))
	copy(corruptedVersion, encoded)
	corruptedVersion[4] = 99
	_, err = DecodeBatch(corruptedVersion)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("expected ErrUnsupportedVersion, got %v", err)
	}

	// 3. CRC checksum mismatch
	corruptedCRC := make([]byte, len(encoded))
	copy(corruptedCRC, encoded)
	corruptedCRC[15] = corruptedCRC[15] ^ 0xFF // Flip a bit in Base Offset to trigger CRC checksum error
	_, err = DecodeBatch(corruptedCRC)
	if !errors.Is(err, ErrCorruptBatch) {
		t.Errorf("expected ErrCorruptBatch for CRC corruption, got %v", err)
	}

	// 4. Truncated data
	if len(encoded) > BatchHeaderSize {
		truncated := encoded[:BatchHeaderSize-1]
		_, err = DecodeBatch(truncated)
		if !errors.Is(err, ErrCorruptBatch) {
			t.Errorf("expected ErrCorruptBatch for truncated data, got %v", err)
		}
	}
}
