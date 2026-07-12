package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/model"
)

var (
	ErrCorruptBatch       = errors.New("corrupt batch: verification failed")
	ErrUnsupportedVersion = errors.New("unsupported format version")
	ErrMalformedMagic     = errors.New("malformed magic bytes")
)

const (
	MagicSize        = 4
	VersionSize      = 1
	BatchLengthSize  = 4
	CRCSize          = 4
	BaseOffsetSize   = 8
	RecordCountSize  = 4
	MaxTimestampSize = 8

	BatchHeaderSize = 33 // 4 + 1 + 4 + 4 + 8 + 4 + 8

	// Minimum bytes remaining after Batch Length field: CRC (4) + Base Offset (8) + Count (4) + Max Timestamp (8) = 24
	MinimumBatchRemainder = BatchHeaderSize - MagicSize - VersionSize - BatchLengthSize

	// Minimum size of an encoded record: Offset Delta (4) + Timestamp Delta (8) + Key Len (4) + Val Len (4) + Header Count (4) = 24
	MinimumRecordSize = 24
)

var MagicBytes = [4]byte{0x50, 0x47, 0x4C, 0x47} // "PGLG"
const FormatVersion1 = uint8(1)

// EncodeBatch serializes a list of stored records into a durable binary batch byte slice.
func EncodeBatch(records []model.StoredRecord) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("cannot encode empty batch")
	}

	baseOffset := records[0].Offset
	var maxTimestamp int64
	for _, r := range records {
		if r.Timestamp > maxTimestamp {
			maxTimestamp = r.Timestamp
		}
	}

	// 1. Calculate record payload bytes to pre-allocate buffer size accurately
	recordsPayloadSize := 0
	for _, r := range records {
		recordsPayloadSize += MinimumRecordSize
		if len(r.Key) > 0 {
			recordsPayloadSize += len(r.Key)
		}
		if len(r.Value) > 0 {
			recordsPayloadSize += len(r.Value)
		}
		for _, h := range r.Headers {
			recordsPayloadSize += 8 + len(h.Key) + len(h.Value) // 4 key len + 4 val len + payloads
		}
	}

	totalSize := BatchHeaderSize + recordsPayloadSize
	buf := make([]byte, totalSize)

	// 2. Write Magic, Version
	copy(buf[0:4], MagicBytes[:])
	buf[4] = FormatVersion1

	// 3. Write Batch Length (all bytes after BatchLength field: CRC (4) + BaseOffset (8) + Count (4) + MaxTimestamp (8) + records size)
	batchLength := uint32(totalSize - 9)
	binary.BigEndian.PutUint32(buf[5:9], batchLength)

	// 4. Populate Header details (leaving CRC zero for now)
	binary.BigEndian.PutUint64(buf[13:21], baseOffset)
	binary.BigEndian.PutUint32(buf[21:25], uint32(len(records)))
	binary.BigEndian.PutUint64(buf[25:33], uint64(maxTimestamp))

	// 5. Write Records
	offset := BatchHeaderSize
	for _, r := range records {
		// Offset Delta
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(r.Offset-baseOffset))
		offset += 4

		// Timestamp Delta (Max Timestamp - Record Timestamp)
		timestampDelta := maxTimestamp - r.Timestamp
		binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(timestampDelta))
		offset += 8

		// Key Length & Key
		if r.Key == nil {
			binary.BigEndian.PutUint32(buf[offset:offset+4], 0xFFFFFFFF)
			offset += 4
		} else {
			binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Key)))
			offset += 4
			copy(buf[offset:offset+len(r.Key)], r.Key)
			offset += len(r.Key)
		}

		// Value Length & Value
		if r.Value == nil {
			binary.BigEndian.PutUint32(buf[offset:offset+4], 0xFFFFFFFF)
			offset += 4
		} else {
			binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Value)))
			offset += 4
			copy(buf[offset:offset+len(r.Value)], r.Value)
			offset += len(r.Value)
		}

		// Header Count & Headers
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Headers)))
		offset += 4

		for _, h := range r.Headers {
			// Key length & key
			binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(h.Key)))
			offset += 4
			copy(buf[offset:offset+len(h.Key)], []byte(h.Key))
			offset += len(h.Key)

			// Value length & value
			binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(h.Value)))
			offset += 4
			copy(buf[offset:offset+len(h.Value)], h.Value)
			offset += len(h.Value)
		}
	}

	// 6. Compute and write CRC32 checksum (covers everything from index 13 to end)
	checksum := crc32.ChecksumIEEE(buf[13:totalSize])
	binary.BigEndian.PutUint32(buf[9:13], checksum)

	return buf[:offset], nil
}

// DecodeBatch decodes a serialized binary batch from the provided data slice.
// It verifies CRC32 and performs extensive boundary/size safety checks.
func DecodeBatch(data []byte) (records []model.StoredRecord, err error) {
	if len(data) < BatchHeaderSize {
		return nil, fmt.Errorf("%w: byte size %d smaller than BatchHeaderSize %d", ErrCorruptBatch, len(data), BatchHeaderSize)
	}

	// 1. Verify Magic
	var magic [4]byte
	copy(magic[:], data[0:4])
	if magic != MagicBytes {
		return nil, ErrMalformedMagic
	}

	// 2. Verify Version
	version := data[4]
	if version != FormatVersion1 {
		return nil, ErrUnsupportedVersion
	}

	// 3. Verify Batch Length
	batchLength := binary.BigEndian.Uint32(data[5:9])
	if batchLength < MinimumBatchRemainder {
		return nil, fmt.Errorf("%w: batch length %d too small", ErrCorruptBatch, batchLength)
	}

	// Prevent overflow check
	totalExpectedSize := int64(MagicSize+VersionSize+BatchLengthSize) + int64(batchLength)
	if int64(len(data)) < totalExpectedSize {
		return nil, fmt.Errorf("%w: expected at least %d bytes, got %d", ErrCorruptBatch, totalExpectedSize, len(data))
	}

	// 4. Verify CRC32 Checksum
	expectedChecksum := binary.BigEndian.Uint32(data[9:13])
	actualChecksum := crc32.ChecksumIEEE(data[13:totalExpectedSize])
	if expectedChecksum != actualChecksum {
		return nil, ErrCorruptBatch
	}

	// 5. Read Header details
	baseOffset := binary.BigEndian.Uint64(data[13:21])
	recordCount := binary.BigEndian.Uint32(data[21:25])
	maxTimestamp := int64(binary.BigEndian.Uint64(data[25:33]))

	// 6. Plausible Record Count sanity checks
	remainderBytes := int64(batchLength) - (BaseOffsetSize + RecordCountSize + MaxTimestampSize)
	if recordCount > uint32(remainderBytes/MinimumRecordSize) {
		return nil, fmt.Errorf("%w: record count %d is impossible for batch size", ErrCorruptBatch, recordCount)
	}

	if recordCount == 0 {
		return []model.StoredRecord{}, nil
	}

	records = make([]model.StoredRecord, recordCount)
	offset := BatchHeaderSize

	for i := uint32(0); i < recordCount; i++ {
		if int64(offset)+MinimumRecordSize > totalExpectedSize {
			return nil, fmt.Errorf("%w: out of bounds reading record %d metadata", ErrCorruptBatch, i)
		}

		// Offset Delta
		offsetDelta := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Timestamp Delta
		timestampDelta := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		if timestampDelta < 0 {
			return nil, fmt.Errorf("%w: negative timestamp delta not allowed", ErrCorruptBatch)
		}
		offset += 8

		// Key Length
		keyLenVal := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		var key []byte
		if keyLenVal > 0 {
			if int64(offset)+int64(keyLenVal) > totalExpectedSize {
				return nil, fmt.Errorf("%w: key overflow on record %d", ErrCorruptBatch, i)
			}
			key = make([]byte, keyLenVal)
			copy(key, data[offset:offset+int(keyLenVal)])
			offset += int(keyLenVal)
		} else if keyLenVal == 0 {
			key = []byte{}
		} else if keyLenVal < -1 {
			return nil, fmt.Errorf("%w: invalid key length %d on record %d", ErrCorruptBatch, keyLenVal, i)
		}

		// Value Length
		valueLenVal := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		var value []byte
		if valueLenVal > 0 {
			if int64(offset)+int64(valueLenVal) > totalExpectedSize {
				return nil, fmt.Errorf("%w: value overflow on record %d", ErrCorruptBatch, i)
			}
			value = make([]byte, valueLenVal)
			copy(value, data[offset:offset+int(valueLenVal)])
			offset += int(valueLenVal)
		} else if valueLenVal == 0 {
			value = []byte{}
		} else if valueLenVal < -1 {
			return nil, fmt.Errorf("%w: invalid value length %d on record %d", ErrCorruptBatch, valueLenVal, i)
		}

		// Header Count
		headerCount := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Sanity check header count
		remainingRecordBytes := totalExpectedSize - int64(offset)
		if headerCount > uint32(remainingRecordBytes/8) { // 8 = minimum header size
			return nil, fmt.Errorf("%w: header count %d exceeds remainder size", ErrCorruptBatch, headerCount)
		}

		var headers []model.Header
		if headerCount > 0 {
			headers = make([]model.Header, headerCount)
			for j := uint32(0); j < headerCount; j++ {
				if int64(offset)+8 > totalExpectedSize {
					return nil, fmt.Errorf("%w: header boundaries overflow on record %d header %d", ErrCorruptBatch, i, j)
				}

				// Header Key Length
				hkLen := binary.BigEndian.Uint32(data[offset : offset+4])
				offset += 4

				if int64(offset)+int64(hkLen) > totalExpectedSize {
					return nil, fmt.Errorf("%w: header key overflow on record %d header %d", ErrCorruptBatch, i, j)
				}
				hk := string(data[offset : offset+int(hkLen)])
				offset += int(hkLen)

				// Header Value Length
				hvLen := binary.BigEndian.Uint32(data[offset : offset+4])
				offset += 4

				if int64(offset)+int64(hvLen) > totalExpectedSize {
					return nil, fmt.Errorf("%w: header value overflow on record %d header %d", ErrCorruptBatch, i, j)
				}
				hv := make([]byte, hvLen)
				copy(hv, data[offset:offset+int(hvLen)])
				offset += int(hvLen)

				headers[j] = model.Header{
					Key:   hk,
					Value: hv,
				}
			}
		}

		records[i] = model.StoredRecord{
			Offset:    baseOffset + uint64(offsetDelta),
			Timestamp: maxTimestamp - timestampDelta,
			Key:       key,
			Value:     value,
			Headers:   headers,
		}
	}

	// Reject trailing unexpected bytes in the declared batch block
	if int64(offset) != totalExpectedSize {
		return nil, fmt.Errorf("%w: trailing unexpected bytes inside batch", ErrCorruptBatch)
	}

	return records, nil
}
