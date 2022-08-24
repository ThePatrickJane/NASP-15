package LSMCompaction

import (
	"encoding/binary"
)

type SSTableElement struct {
	CRC       [4]byte
	Timestamp [16]byte
	Tombstone [1]byte
	KeySize   [8]byte
	ValueSize [8]byte
	Key       []byte
	Value     []byte
}

func (el *SSTableElement) GetKeySize() uint64 {
	return binary.BigEndian.Uint64(el.KeySize[:])
}

func (el *SSTableElement) GetValueSize() uint64 {
	return binary.BigEndian.Uint64(el.ValueSize[:])
}

func (el *SSTableElement) GetKey() string {
	return string(el.Key)
}

func (el *SSTableElement) CheckNewer(other SSTableElement) bool {
	for i := 0; i < 16; i++ {
		if el.Timestamp[i] > other.Timestamp[i] {
			return true
		} else if el.Timestamp[i] < other.Timestamp[i] {
			return false
		}
	}
	return true
}

func (el *SSTableElement) GetAsByteArray() []byte {
	elAsBytes := make([]byte, 0)
	elAsBytes = append(elAsBytes, el.CRC[:]...)
	elAsBytes = append(elAsBytes, el.Timestamp[:]...)
	elAsBytes = append(elAsBytes, el.Tombstone[:]...)
	elAsBytes = append(elAsBytes, el.KeySize[:]...)
	elAsBytes = append(elAsBytes, el.ValueSize[:]...)
	elAsBytes = append(elAsBytes, el.Key...)
	elAsBytes = append(elAsBytes, el.Value...)
	return elAsBytes
}
