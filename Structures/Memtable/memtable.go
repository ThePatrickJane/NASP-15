package Memtable

import (
	skiplist "Projekat/Structures/SkipList"
	"errors"
)

type Memtable struct {
	data *skiplist.SkipList
	size int
}

const (
	TOMBSTONE_SIZE = 1
	TIMESTAMP_SIZE = 16
)

func New(maxHeight int, size int) *Memtable {
	return &Memtable{
		data: skiplist.New(maxHeight),
		size: size,
	}
}

func (memtable *Memtable) Add(key string, value []byte) (skiplist.Content, error) {
	// racunanje velicine elementa koji ce se dodati
	elementSize := len(key) + len(value) + TOMBSTONE_SIZE + TIMESTAMP_SIZE

	// provera da li ima mesta u memtable
	if memtable.size >= memtable.size+elementSize {
		memtable.size = memtable.size + elementSize
		return memtable.data.Add(key, value), nil
	}
	return skiplist.Content{}, errors.New("max size reached, flush the data")
}

func (memtable *Memtable) Delete(key string) (skiplist.Content, error) {

	// preuzimanje elementa koji zelimo da obrisemo
	content1, err1 := memtable.data.Get(key)
	if err1 != nil {
		return skiplist.Content{}, err1
	}

	// racunanje velicine elementa
	elementSize := len(key) + len(content1.Value) + TOMBSTONE_SIZE + TIMESTAMP_SIZE
	content2, err2 := memtable.data.Delete(key)
	if err2 != nil {
		return skiplist.Content{}, err2
	}
	// smanjenje zauzete velicine u memtable
	memtable.size = memtable.size - elementSize
	return content2, nil
}

func (memtable *Memtable) Get(key string) (skiplist.Content, error) {
	return memtable.data.Get(key)
}

func (memtable *Memtable) Update(key string, value []byte) (skiplist.Content, error) {

	// preuzimanje elementa kojeg zelimo da update
	content, err := memtable.data.Get(key)
	if err != nil {
		return skiplist.Content{}, err
	}
	// Trenutna duzina elementa
	currentSize := len(content.Value) + len(key) + TOMBSTONE_SIZE + TIMESTAMP_SIZE

	// Nova duzina elementa
	newSize := len(value) + len(key) + TOMBSTONE_SIZE + TIMESTAMP_SIZE
	sizeDifference := currentSize - newSize

	// provera da li nova vrednost moze stati u memtable
	if memtable.size >= memtable.size+sizeDifference {
		updatedElement, err := memtable.data.Update(key, value)
		if err != nil {
			return skiplist.Content{}, nil
		}
		return updatedElement, nil
	}
	return skiplist.Content{}, errors.New("no space in memtable")
}

func (memtable *Memtable) Serialize() map[string]skiplist.Content {
	return memtable.data.ToMap()
}

func (memtable *Memtable) Clear() {
	memtable.size = 0
	memtable.data.Clear()
}
