package TokenBucket

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

type TokenBucket struct {
	Max uint64
	Remaining uint64
	Interval int64
	NextResetTime int64
}

func (tb *TokenBucket) UseToken() bool {
	if time.Now().Unix() > tb.NextResetTime {
		tb.Remaining = tb.Max
		tb.NextResetTime = time.Now().Unix() + tb.Interval
	}

	if tb.Remaining == 0 {
		return false
	}

	tb.Remaining -= 1

	return true
}

func (tb *TokenBucket) Serialize() []byte {
	ret := bytes.Buffer{}
	enc := gob.NewEncoder(&ret)
	err := enc.Encode(tb)
	if err != nil {
		panic(err)
	}
	return ret.Bytes()
}

func (tb *TokenBucket) Deserialize(bajtovi []byte) {
	temp := bytes.Buffer{}
	temp.Write(bajtovi)
	dec := gob.NewDecoder(&temp)
	err := dec.Decode(tb)
	if err != nil {
		panic(err)
	}
}

func MakeTokenBucket(max uint64, interval int64) TokenBucket {
	return TokenBucket{
		Max:           max,
		Remaining:     max,
		Interval:      interval,
		NextResetTime: time.Now().Unix() + interval,
	}
}

func TokenBucketProba() {
	tb := MakeTokenBucket(4, 2)
	fmt.Println(tb)
	for i := 1; i <= 10; i++ {
		if i == 6 || i == 8 {
			fmt.Println("cekam")
			time.Sleep(3 * time.Second)
		}
		fmt.Println(i, tb.UseToken())
	}
	fmt.Println(tb)

	f, err := os.Create("tb_ser.bin")
	if err != nil {
		panic(err)
	}
	bajtovi := tb.Serialize()
	num, _ := f.Write(bajtovi)
	fmt.Println("upisano bajtova", num)

	bajtovi, err = os.ReadFile("tb_ser.bin")
	fmt.Println("procitano:", len(bajtovi))
	newTb := TokenBucket{}
	newTb.Deserialize(bajtovi)

	fmt.Println(newTb)
}
