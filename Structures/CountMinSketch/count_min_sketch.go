package CountMinSketch

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"time"
)

type CountMinSketch struct {
	Epsilon float64
	Delta float64
	M uint
	K         uint
	HashFuncs []hash.Hash32
	Matrix [][]uint
	Time   uint
}

func (cms *CountMinSketch) CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func (cms *CountMinSketch) CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func (cms *CountMinSketch) GenerateHashFuncs(k uint) []hash.Hash32 {
	h := []hash.Hash32{}
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(cms.Time+ i)))
	}
	return h
}

func (cms *CountMinSketch) Add(el string) {
	for i, h := range cms.HashFuncs {
		h.Reset()
		h.Write([]byte(el))
		index := h.Sum32() % uint32(cms.M)
		cms.Matrix[i][index] += 1
	}
}

func (cms *CountMinSketch) EstimateFrequency(el string) uint {
	min := uint(math.Inf(1))
	for i, h := range cms.HashFuncs {
		h.Reset()
		h.Write([]byte(el))
		index := h.Sum32() % uint32(cms.M)
		if cms.Matrix[i][index] < min {
			min = cms.Matrix[i][index]
		}
	}
	return min
}

func (cms CountMinSketch) Serialize() []byte {
	ret := bytes.Buffer{}
	enc := gob.NewEncoder(&ret)
	cms.HashFuncs = nil
	err := enc.Encode(cms)
	if err != nil {
		panic(err)
	}
	return ret.Bytes()
}

func (cms *CountMinSketch) Deserialize(data []byte) {
	temp := bytes.Buffer{}
	temp.Write(data)
	dec := gob.NewDecoder(&temp)
	err := dec.Decode(cms)
	if err != nil {
		panic(err)
	}
	cms.HashFuncs = cms.GenerateHashFuncs(cms.K)
}

func MakeCountMinSketch(epsilon float64, delta float64) CountMinSketch {
	cms := CountMinSketch{Epsilon: epsilon, Delta: delta}
	cms.M = cms.CalculateM(epsilon)
	cms.K = cms.CalculateK(delta)
	cms.Time = uint(time.Now().Unix())
	cms.HashFuncs = cms.GenerateHashFuncs(cms.K)
	cms.Matrix = make([][]uint, cms.K)
	for i := range cms.Matrix {
		cms.Matrix[i] = make([]uint, cms.M)
	}
	return cms
}

func GetTestCMS() CountMinSketch {
	cms := MakeCountMinSketch(0.1, 0.1)
	cms.Add("milos")
	cms.Add("milos")
	cms.Add("milos")
	cms.Add("milos")
	cms.Add("mladen")
	cms.Add("mladen")
	cms.Add("pera")
	cms.Add("pera")
	return cms
}

func CountMinSketchProba() {
	cms := MakeCountMinSketch(0.1, 0.1)
	cms.Add("milos")
	cms.Add("milos")
	cms.Add("milos")
	cms.Add("milos")
	cms.Add("mladen")
	cms.Add("mladen")
	cms.Add("pera")
	cms.Add("pera")
	fmt.Println(cms)
	fmt.Println(cms.EstimateFrequency("pera"))

	f, err := os.Create("cms_ser.bin")
	if err != nil {
		panic(err)
	}
	bajtovi := cms.Serialize()
	num, _ := f.Write(bajtovi)
	fmt.Println("upisano bajtova", num)

	bajtovi, err = os.ReadFile("cms_ser.bin")
	fmt.Println("procitano:", len(bajtovi))
	newCms := CountMinSketch{}
	newCms.Deserialize(bajtovi)
	fmt.Println(newCms)

	fmt.Println(cms.EstimateFrequency("pera"))
	fmt.Println(cms.EstimateFrequency("milos"))
	cms.Add("pera")
	fmt.Println(cms.EstimateFrequency("pera"))

}
