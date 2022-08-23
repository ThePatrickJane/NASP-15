package BloomFilter

import (
	"bytes"
	"encoding/gob"
	_ "encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"time"
)

type BloomFilter struct {
	ExpectedElements  int
	FalsePositiveRate float64
	M                 uint
	K         uint
	HashFuncs []hash.Hash32
	BitSet    []int
	Time      uint
}

func (bf *BloomFilter) CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func (bf *BloomFilter) CalculateK(expectedElements int, M uint) uint {
	return uint(math.Ceil((float64(M) / float64(expectedElements)) * math.Log(2)))
}

func (bf *BloomFilter) GenerateHashFuncs(K uint) []hash.Hash32 {
	h := []hash.Hash32{}
	for i := uint(0); i < K; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(bf.Time + i)))
	}
	return h
}

func (bf *BloomFilter) Add(el string) {
	for _, h := range bf.HashFuncs {
		h.Reset()
		h.Write([]byte(el))
		index := h.Sum32() % uint32(bf.M)
		bf.BitSet[index] = 1
	}
}

func (bf *BloomFilter) Search(el string) bool {
	for _, h := range bf.HashFuncs {
		h.Reset()
		h.Write([]byte(el))
		index := h.Sum32() % uint32(bf.M)
		if bf.BitSet[index] == 0 {
			return false
		}
	}
	return true
}

func (bf BloomFilter) Serialize() []byte {
	ret := bytes.Buffer{}
	enc := gob.NewEncoder(&ret)
	bf.HashFuncs = nil
	err := enc.Encode(bf)
	if err != nil {
		panic(err)
	}
	return ret.Bytes()
}

func (bf *BloomFilter) Deserialize(data []byte) {
	temp := bytes.Buffer{}
	temp.Write(data)
	dec := gob.NewDecoder(&temp)
	err := dec.Decode(bf)
	if err != nil {
		panic(err)
	}
	bf.HashFuncs = bf.GenerateHashFuncs(bf.K)
}

func MakeBloomFilter(expectedElements int, falsePositiveRate float64) BloomFilter {
	bf := BloomFilter{ExpectedElements: expectedElements, FalsePositiveRate: falsePositiveRate}
	bf.M = bf.CalculateM(expectedElements, falsePositiveRate)
	bf.K = bf.CalculateK(expectedElements, bf.M)
	bf.Time = uint(time.Now().Unix())
	bf.HashFuncs = bf.GenerateHashFuncs(bf.K)
	bf.BitSet = make([]int, bf.M)
	return bf
}

func BloomFilterProba() {
	bf := MakeBloomFilter(10, 0.1)
	//fmt.Println(bf)
	bf.Add("milos")
	bf.Add("pera")
	bf.Add("stoja")
	bf.Add("mladen")
	fmt.Println(bf)
	fmt.Println(bf.Search("milos"))
	fmt.Println(bf.Search("zika"))
	f, err := os.Create("bloom_ser.bin")
	if err != nil {
		panic(err)
	}
	bajtovi := bf.Serialize()
	num, _ := f.Write(bajtovi)
	fmt.Println("upisano bajtova", num)

	bajtovi, err = os.ReadFile("bloom_ser.bin")
	fmt.Println("procitano:", len(bajtovi))
	newBloom := BloomFilter{}
	newBloom.Deserialize(bajtovi)
	fmt.Println(newBloom)
	newBloom.Add("joko")
	fmt.Println(newBloom.Search("djoka"))
	fmt.Println(newBloom.Search("mika"))
	fmt.Println(newBloom.Search("rokok"))
	fmt.Println(newBloom.Search("toja"))
	fmt.Println(newBloom.Search("joko"))
}

