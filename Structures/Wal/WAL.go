package Wal

import (
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/spaolacci/murmur3"
	"hash"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type Segment struct {
	crc       []byte
	timestamp []byte
	tombstone []byte
	keysize   []byte
	valuesize []byte
	key       []byte
	value     []byte
}

// Konstruktor za segment
func (segment *Segment) construct(key []byte, value []byte, tombstone uint8) {
	segment.key = key
	segment.value = value
	segment.crc = make([]byte, 4)
	binary.LittleEndian.PutUint32(segment.crc, uint32(CRC32(key)))
	segment.timestamp = make([]byte, 16)
	binary.LittleEndian.PutUint64(segment.timestamp, uint64(time.Now().Unix()))
	segment.tombstone = make([]byte, 1)
	segment.tombstone[0] = byte(tombstone)
	segment.keysize = make([]byte, 8)
	binary.LittleEndian.PutUint64(segment.keysize, uint64(len(key)))
	segment.valuesize = make([]byte, 8)
	binary.LittleEndian.PutUint64(segment.valuesize, uint64(len(value)))
}

type WAL struct {
	segments         []Segment
	file_path        string
	treshold         int8
	segment_treshold int8
	file_num         int
}

// Konstruktor za WAL
func (wal *WAL) constuct() {
	wal.treshold = 0
	wal.segment_treshold = 0
	wal.readMMap()
}

// Funkcija za reagovanje na greske
func isError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}
	return (err != nil)
}

// Dodavanje elementa u WAL
func (wal *WAL) append(key []byte, value []byte, tombstone uint8) {
	segment := Segment{}
	segment.construct(key, value, tombstone)
	//println(binary.Size(segment))
	wal.segments = append(wal.segments, segment)
	wal.treshold += 1
	wal.segment_treshold += 1
	//Na 5 segmenta pravi novi Wal file
	if wal.segment_treshold >= 5 {
		wal.write_to_file_MMap(segment)
		wal.segment_treshold = 0
		return
	}
	file, err := os.OpenFile(wal.file_path, os.O_RDWR, 0600)
	if isError(err) {
		return
	}
	wal.writeMMap(file, segment)
}

func (wal *WAL) readMMap() {
	files, err := ioutil.ReadDir("Wal")
	if err != nil {
		log.Fatal(err)
	}

	//Ako ne postoje Wal fajlovi napravi novi
	//if len(files) == 0 {
	//	_, err := os.OpenFile("Wal\\WAL1", os.O_CREATE, 0600)
	//	wal.mapa = nil
	//	if isError(err) {
	//		return
	//	}
	//	wal.file_num = 1
	//	wal.file_path = "Wal\\WAL1"
	//	return
	//}
	wal.file_num = len(files)
	file, err := os.OpenFile("Wal\\Wal"+strconv.Itoa(len(files)), os.O_RDWR, 0644)
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	defer mmapf.Unmap()
	wal.file_path = "Wal\\Wal" + strconv.Itoa(len(files))
	if mmapf == nil {
		return
	}
	result := make([]byte, len(mmapf))
	copy(result, mmapf)
	start := 0
	end := 37
	new_reading_size := 0
	for {
		println(end + new_reading_size)
		println(mmapf)
		if end+new_reading_size > len(mmapf) {
			break
		}
		velicina_kljuca := binary.LittleEndian.Uint64(result[start+21 : start+29])
		velicina_vrednosti := binary.LittleEndian.Uint64(result[start+29 : end])
		new_reading_size = int(velicina_kljuca + velicina_vrednosti)
		key := string(result[end : end+int(velicina_kljuca)])
		value := string(result[end+int(velicina_kljuca) : end+int(new_reading_size)])
		start = end + int(new_reading_size)
		end = start + 37
		println("Kljuc:", key)
		println("Vrednost:", value)
		wal.segment_treshold += 1
	}

}

//func (wal *Wal) read() {
//	//Ucitavanje wal fajlova
//	files, err := ioutil.ReadDir("Wal")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	//Ako ne postoje Wal fajlovi napravi novi
//	if len(files) == 0 {
//		_, err := os.OpenFile("Wal\\WAL1", os.O_CREATE, 0600)
//		//mmapf, err := mmap.Map(file, mmap.RDWR, 0)
//		//wal.mapa = mmapf
//		if isError(err) {
//			return
//		}
//		wal.file_num = 1
//		wal.file_path = "Wal\\WAL1"
//		return
//	}
//
//	//Otvaranje zadnjeg wal fajla
//	wal.file_num = len(files)
//	file, err := os.OpenFile("Wal\\Wal"+strconv.Itoa(len(files)), os.O_RDONLY, 0644)
//	//mmapf, err := mmap.Map(file, mmap.RDWR, 0)
//	//wal.mapa = mmapf
//	if isError(err) {
//		return
//	}
//	wal.file_path = "Wal\\Wal" + strconv.Itoa(len(files))
//	reader := bufio.NewReader(file)
//	//Citanje zapisa iz zadnjeg Wal segmenta
//	for {
//		byteSlice := make([]byte, 37)
//		bytesRead, err := reader.Read(byteSlice)
//		if isError(err) {
//			return
//		}
//		velicina_kljuca := binary.LittleEndian.Uint64(byteSlice[21:29])
//		velicina_vrednosti := binary.LittleEndian.Uint64(byteSlice[29:])
//		new_reading_size := velicina_kljuca + velicina_vrednosti
//		byteSlice = make([]byte, new_reading_size)
//		bytesRead, err = reader.Read(byteSlice)
//		key := string(byteSlice[0:velicina_kljuca])
//		value := string(byteSlice[velicina_kljuca:])
//		println("Procitano bajta:", bytesRead)
//		println("Kljuc:", key)
//		println("Vrednost:", value)
//
//		wal.segment_treshold += 1
//	}
//	file.Close()
//}

func (wal *WAL) write_to_file_MMap(segment Segment) {
	file, err := os.OpenFile("Wal\\Wal"+strconv.Itoa(int(wal.file_num+1)), os.O_RDWR|os.O_CREATE, 0600)
	wal.file_path = "Wal\\Wal" + strconv.Itoa(int(wal.file_num+1))
	if isError(err) {
		return
	}
	wal.writeMMap(file, segment)
	file.Close()
}

func (wal *WAL) writeMMap(file *os.File, segment Segment) {
	currentLen, err := fileLen(file)
	sz := 0
	if isError(err) {
		return
	}
	sz += binary.Size(segment.crc)
	sz += binary.Size(segment.timestamp)
	sz += binary.Size(segment.tombstone)
	sz += binary.Size(segment.keysize)
	sz += binary.Size(segment.valuesize)
	sz += binary.Size(segment.key)
	sz += binary.Size(segment.value)

	err = file.Truncate(currentLen + int64(sz))
	if isError(err) {
		return
	}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	defer mmapf.Unmap()
	//if wal.mapa == nil {
	//	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	//	if isError(err) {
	//		return
	//	}
	//	wal.mapa = mmapf
	//}
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.crc))], segment.crc)
	currentLen += int64(binary.Size(segment.crc))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.timestamp))], segment.timestamp)
	currentLen += int64(binary.Size(segment.timestamp))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.tombstone))], segment.tombstone)
	currentLen += int64(binary.Size(segment.tombstone))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.keysize))], segment.keysize)
	currentLen += int64(binary.Size(segment.keysize))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.valuesize))], segment.valuesize)
	currentLen += int64(binary.Size(segment.valuesize))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.key))], segment.key)
	currentLen += int64(binary.Size(segment.key))
	copy(mmapf[currentLen:], segment.value)
	currentLen += int64(binary.Size(segment.value))
}

func (wal *WAL) write_to_file(file *os.File) {
	for _, segment := range wal.segments {
		wal.write(file, segment)
	}
	file_1, err := os.OpenFile("Wal\\Wal"+strconv.Itoa(int(wal.file_num+1)), os.O_CREATE, 0600)
	if isError(err) {
		return
	}
	file_1.Close()

}

func (wal *WAL) write(file *os.File, segment Segment) {
	err := binary.Write(file, binary.LittleEndian, segment.crc)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.LittleEndian, segment.timestamp)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.LittleEndian, segment.tombstone)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.LittleEndian, segment.keysize)
	println(binary.LittleEndian.Uint64(segment.keysize))
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.LittleEndian, segment.valuesize)
	println(binary.LittleEndian.Uint64(segment.valuesize))
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.LittleEndian, segment.key)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.LittleEndian, segment.value)
	if err != nil {
		log.Fatal("Write failed")
	}
}

const (
	TRESHOLD         = 2
	SEGMENT_TRESHOLD = 5
	T_SIZE           = 8
	C_SIZE           = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func CreateHashFunctionsS(k uint) []hash.Hash32 {
	h := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h
}

func SSTableProba() {
	wal := WAL{}
	wal.constuct()
	//wal.append([]byte("1"), []byte("asdfsdf"), 1)
	//wal.append([]byte("123"), []byte("noicee"), 1)
	//wal.append([]byte("1s"), []byte("asdfsdf1231"), 1)
	//wal.append([]byte("123fd"), []byte("noicee4363"), 1)
	//wal.append([]byte("1dfg"), []byte("asdfsdf6568"), 1)
	wal.readMMap()
}

func main() {
	SSTableProba()
}
