package Memtable

import (
	"Projekat/Structures/SSTable"
	skiplist "Projekat/Structures/SkipList"
	"errors"
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

func (memtable *Memtable) Add(key string, value []byte) (skiplist.Content, error) {
	// racunanje velicine elementa koji ce se dodati

	// provera da li ima mesta u memtable
	if memtable.maxElNumber > memtable.curElNumber+1 {
		memtable.curElNumber = memtable.curElNumber + 1
		return memtable.data.Add(key, value), nil
	}
	return skiplist.Content{}, errors.New("max size reached, flush the data")
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
	memtable.data.Add(key, value)
}
func (memtable *Memtable) Flush(sstable SSTable.SSTable) {
	sstable.Flush(memtable.data.GetElements())
}

//func metoda([]Wal.Segment) {
//
//}

func (memtable *Memtable) Update(key string, value []byte) (skiplist.Content, error) {

	// preuzimanje elementa kojeg zelimo da update i update
	updatedElement, err := memtable.data.Update(key, value)
	if err != nil {
		return skiplist.Content{}, err
	}
	return updatedElement, nil

}

func (memtable *Memtable) Serialize() map[string]skiplist.Content {
	return memtable.data.ToMap()
}

func (memtable *Memtable) Clear() {
	memtable.curElNumber = 0
	memtable.data.Clear()
}
