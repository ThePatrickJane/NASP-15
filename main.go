package main

import (
	//. "Projekat/Structures/BloomFilter"
	//. "Projekat/Structures/Cache"
	//. "Projekat/Structures/CountMinSketch"
	//. "Projekat/Structures/HyperLogLog"
	//. "Projekat/Structures/TokenBucket"
	. "Projekat/Settings"
	"fmt"
)

func main() {
	//BloomFilterProba()
	//CountMinSketchProba()
	//HyperLogLogProba()
	//CacheProba()
	//TokenBucketProba()
	settings := Settings{Path: "settings.json"}
	settings.LoadFromJSON()
	fmt.Println(settings)
}
