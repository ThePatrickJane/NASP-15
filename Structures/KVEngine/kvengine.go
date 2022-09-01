package KVEngine

import (
	"Projekat/Settings"
	"Projekat/Structures/Cache"
	"Projekat/Structures/LSMCompaction"
	"Projekat/Structures/Memtable"
	"Projekat/Structures/SSTable"
	"Projekat/Structures/TokenBucket"
	"Projekat/Structures/Wal"
	"fmt"
)

type KVEngine struct {
	tokenBucket TokenBucket.TokenBucket
	cache       Cache.Cache
	wal         Wal.WAL
	memtable    Memtable.Memtable
}

func (kve *KVEngine) Get(key string) (bool, []byte) {

	if !kve.tokenBucket.UseToken() {
		fmt.Println("Nema dovoljno tokena.")
		return false, nil
	}

	if content, err := kve.memtable.Get(key); err == nil {

		if content.Tombstone {
			fmt.Println("Logicki je obrisano.")
			return false, nil
		}

		fmt.Println("Nadjeno u memtable.")
		return true, content.Value
	}
	//fmt.Println(kve.cache)
	if found, data := kve.cache.Get(key); found {
		fmt.Println("Nadjeno u cache.")
		return true, data
	}

	if data := SSTable.Find(key); data != nil {
		fmt.Println("Nadjeno u data.")
		kve.cache.Put(key, data)
		return true, data
	}

	return false, nil
}

func (kve *KVEngine) Put(key string, data []byte) bool {

	if !kve.tokenBucket.UseToken() {
		fmt.Println("Nema dovoljno tokena.")
		return false
	}

	kve.wal.Insert([]byte(key), data, 0)

	kve.memtable.Add(key, data, false)

	return true
}

func (kve *KVEngine) Delete(key string) bool {

	if !kve.tokenBucket.UseToken() {
		fmt.Println("Nema dovoljno tokena.")
		return false
	}

	kve.wal.Insert([]byte(key), make([]byte, 0), 1)

	if deleted := kve.memtable.LogDelete(key); !deleted {
		kve.memtable.Add(key, make([]byte, 0), true)
	}

	if found, _ := kve.cache.Get(key); found {
		kve.cache.Remove(key)
	}

	return true
}

func (kve *KVEngine) ReconstructMemtable() {
	kve.memtable.Reconstruction(Wal.ReadLastSegment())
}

func (kve *KVEngine) Compactions() {
	LSMCompaction.LSMCompaction(1)
}

func MakeKVEngine() KVEngine {
	settings := Settings.Settings{Path: "settings.json"}
	settings.LoadFromJSON()

	wal := Wal.WAL{}
	wal.Constuct(int(settings.MemtableMaxElements), int(settings.WalMaxSegments))

	kvengine := KVEngine{}
	kvengine.cache = Cache.MakeCache(uint64(settings.CacheMaxElements))
	kvengine.tokenBucket = TokenBucket.MakeTokenBucket(uint64(settings.TokenBucketMaxTokens), int64(settings.TokenBucketInterval))
	kvengine.wal = wal
	kvengine.memtable = *Memtable.New(5, int(settings.MemtableMaxElements))

	//kvengine.ReconstructMemtable()

	return kvengine
}
