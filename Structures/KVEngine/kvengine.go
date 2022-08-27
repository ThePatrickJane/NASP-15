package KVEngine

import (
	"Projekat/Structures/Cache"
	"Projekat/Structures/TokenBucket"
)

type KVEngine struct {
	tokenBucket TokenBucket.TokenBucket
	cache Cache.Cache
}

func (kve *KVEngine) Get(key string) (bool, []byte) {
	return false, nil
}

func (kve *KVEngine) Put(key string, data []byte) bool {
	return true
}

func (kve *KVEngine) Delete(key string) bool {
	return true
}

func MakeKVEngine() KVEngine {
	kvengine := KVEngine{}
	return kvengine
}
