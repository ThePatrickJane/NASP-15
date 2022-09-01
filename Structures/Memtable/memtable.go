package Memtable

import (
	"Projekat/Structures/SSTable"
	skiplist "Projekat/Structures/SkipList"
	"Projekat/Structures/Wal"
	"encoding/binary"
	"strconv"
)

type Memtable struct {
	data        *skiplist.SkipList
	maxElNumber int
	curElNumber int
}

func New(maxHeight int, maxElNumber int) *Memtable {
	return &Memtable{
		data:        skiplist.New(maxHeight),
		maxElNumber: maxElNumber,
	}
}

func (memtable *Memtable) Add(key string, value []byte, tombstone bool) {
	// racunanje velicine elementa koji ce se dodati

	memtable.data.Add(key, value, tombstone)
	memtable.curElNumber += 1

	// provera da li ima mesta u memtable
	if memtable.curElNumber == memtable.maxElNumber {
		memtable.Flush()
	}
}

func (memtable *Memtable) Delete(key string) (skiplist.Content, error) {

	// brisanje
	content2, err2 := memtable.data.Delete(key)
	if err2 != nil {
		return skiplist.Content{}, err2
	}
	// smanjenje zauzete velicine u memtable
	memtable.curElNumber = memtable.curElNumber - 1
	return content2, nil
}

func (memtable *Memtable) Get(key string) (skiplist.Content, error) {
	return memtable.data.Get(key)
}

func (memtable *Memtable) BrziAdd(key string, value []byte) {
	memtable.data.Add(key, value, false)
}
func (memtable *Memtable) Flush() {
	sstable := SSTable.SSTable{}
	sstable.Construct()
	sstable.Flush(memtable.data.GetElements())
	memtable.Clear()
	//fmt.Println("Flushavano")
}

func (memtable *Memtable) Reconstruction(segments []Wal.Segment) {
	memtable.Clear()
	for _, segment := range segments {

		key := (string)(segment.Key)
		value := segment.Value
		tombstone, _ := strconv.ParseBool(string(segment.TombStone))
		timestamp := (int64)(binary.BigEndian.Uint64(segment.TimeStamp))
		memtable.data.ReconstructionInsert(key, value, tombstone, timestamp)
	}
}

func (memtable *Memtable) Update(key string, value []byte) (skiplist.Content, error) {

	// preuzimanje elementa kojeg zelimo da update i update
	updatedElement, err := memtable.data.Update(key, value)
	if err != nil {
		return skiplist.Content{}, err
	}
	return updatedElement, nil

}

func (memtable *Memtable) LogDelete(key string) bool {
	return memtable.data.LogDelete(key)
}

func (memtable *Memtable) Serialize() map[string]skiplist.Content {
	return memtable.data.ToMap()
}

func (memtable *Memtable) Clear() {
	memtable.curElNumber = 0
	memtable.data.Clear()
}
