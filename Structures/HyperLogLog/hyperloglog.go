package HyperLogLog

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"strconv"
)

type HyperLogLog struct {
	M   uint64
	P    uint8
	Regs []uint8
}

func (hll *HyperLogLog) Add(el string) {
	hashed := ToBinary(GetMD5Hash(el))[:18]
	bucketInBinary := hashed[:hll.P]
	bucket, _ := strconv.ParseInt(bucketInBinary, 2, 64)

	zeros := uint8(0)
	for i := len(hashed) - 1; i >= 0; i-- {
		if string(hashed[i]) == "0" {
			zeros += 1
		} else {
			break
		}
	}
	//fmt.Println(el, hashed, bucketInBinary, bucket, zeros)

	if zeros > hll.Regs[bucket] {
		hll.Regs[bucket] = zeros
	}
}

func (hll *HyperLogLog) EmptyRegs() uint8 {
	sum := uint8(0)
	for _, val := range hll.Regs {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HyperLogLog) EstimateCardinality() float64 {
	sum := 0.0
	for _, val := range hll.Regs {
		sum += math.Pow(2.0, -float64(val))
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.EmptyRegs()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation

	//hm := float64(hll.M) / sum
	//e := 0.7213 * float64(hll.M) * hm
	//if e < 2.5 * float64(hll.M) {
	//	if hll.EmptyRegs() > 0 {
	//		return -float64(hll.M) * math.Log(float64(hll.EmptyRegs()) / float64(hll.M))
	//	}
	//} else if e > math.Pow(2.0, float64(hll.M)) / 30 {
	//	return -math.Pow(2.0, float64(hll.M)) * math.Log(1.0 - e / math.Pow(2.0, float64(hll.M)))
	//}
	//return e
}

func (hll HyperLogLog) Serialize() []byte {
	ret := bytes.Buffer{}
	enc := gob.NewEncoder(&ret)
	err := enc.Encode(hll)
	if err != nil {
		panic(err)
	}
	return ret.Bytes()
}

func (hll *HyperLogLog) Deserialize(data []byte) {
	temp := bytes.Buffer{}
	temp.Write(data)
	dec := gob.NewDecoder(&temp)
	err := dec.Decode(hll)
	if err != nil {
		panic(err)
	}
}

func MakeHyperLogLog(p uint8) HyperLogLog {
	hll := HyperLogLog{P: p}
	hll.M = uint64(math.Pow(2.0, float64(p)))
	hll.Regs = make([]uint8, hll.M)
	return hll
}

func GetTestHLL() HyperLogLog {
	hll := MakeHyperLogLog(7)
	hll.Add("proba")
	hll.Add("proba")
	hll.Add("proba1")
	hll.Add("proba1")
	hll.Add("proba2")
	hll.Add("proba")
	hll.Add("proba1")
	hll.Add("proba3")
	hll.Add("proba4")
	hll.Add("proba5")
	hll.Add("proba6")
	return hll
}

func HyperLogLogProba() {
	hll := MakeHyperLogLog(7)
	hll.Add("lkjh mi trava je zelna")
	hll.Add("lkjh mi trava je zelna")
	hll.Add("lkjh mi sd")
	hll.Add("lkjh mi sd")
	hll.Add(";;9 8 kaa yb by ")
	hll.Add("lkjh mi trava je zelna")
	hll.Add("lkjh mi sd")
	hll.Add("jfgh mfghfgh")
	hll.Add("kgfdfert")
	hll.Add("kdrerasdf")
	hll.Add("lasdjuwe sd")

	fmt.Println("kar:", hll.EstimateCardinality())
	fmt.Println(hll)

	f, err := os.Create("hll_ser.bin")
	if err != nil {
		panic(err)
	}
	bajtovi := hll.Serialize()
	num, _ := f.Write(bajtovi)
	fmt.Println("upisano bajtova", num)

	bajtovi, err = os.ReadFile("hll_ser.bin")
	fmt.Println("procitano:", len(bajtovi))
	newHll := HyperLogLog{}
	newHll.Deserialize(bajtovi)
	newHll.Add("dfg")
	newHll.Add("jdfg")
	newHll.Add("34634")
	newHll.Add("lhfgdr3")
	newHll.Add("463")
	newHll.Add("hf")
	newHll.Add("kajsd")

	newHll.Add("lnvg")
	newHll.Add("masdg")
	newHll.Add("keva")
	newHll.Add("cale")
	newHll.Add(";''j8234")
	newHll.Add(",sldf kjsdfks dhfkalsjdlk")
	newHll.Add("82oi3je woifhqwgqojwl f")
	fmt.Println(newHll)
	fmt.Println("kar:", newHll.EstimateCardinality())
}