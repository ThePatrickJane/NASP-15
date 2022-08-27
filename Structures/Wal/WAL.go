package Wal

import (
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/spaolacci/murmur3"
	"hash"
	"hash/crc32"
	"log"
	"os"
	"strconv"
	"strings"
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
func (segment *Segment) Construct(key []byte, value []byte, tombstone uint8) {
	segment.key = key
	segment.value = value
	segment.crc = make([]byte, 4)
	binary.BigEndian.PutUint32(segment.crc, uint32(CRC32(key)))
	segment.timestamp = make([]byte, 16)
	binary.BigEndian.PutUint64(segment.timestamp, uint64(time.Now().Unix()))
	segment.tombstone = make([]byte, 1)
	segment.tombstone[0] = byte(tombstone)
	segment.keysize = make([]byte, 8)
	binary.BigEndian.PutUint64(segment.keysize, uint64(len(key)))
	segment.valuesize = make([]byte, 8)
	binary.BigEndian.PutUint64(segment.valuesize, uint64(len(value)))
}

type WAL struct {
	segments             []Segment
	file_path            string
	treshold             int8
	segment_treshold     int8
	file_num             int
	maxElementsInSegment int
	maxSegments          int
}

// Konstruktor za WAL
func (wal *WAL) Constuct(maxElements int, maxSegments int) {
	wal.treshold = 0
	wal.segment_treshold = 0
	wal.maxElementsInSegment = maxElements
	wal.file_path = "./Data/WAL1.db"
	wal.maxSegments = maxSegments
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
	segment.Construct(key, value, tombstone)
	//println(binary.Size(segment))
	wal.segments = append(wal.segments, segment)
	wal.treshold += 1
	wal.segment_treshold += 1
	//Na 5 segmenta pravi novi Wal file
	if int(wal.segment_treshold) > wal.maxElementsInSegment {
		wal.write_to_file_MMap(segment)
		wal.segment_treshold = 0
		if wal.file_num > wal.maxSegments {
			wal.deleteOldSegments()
		}
		return
	}
	file, err := os.OpenFile(wal.file_path, os.O_RDWR|os.O_CREATE, 0600)
	if isError(err) {
		return
	}
	wal.writeMMap(file, segment)
}

func (wal *WAL) readMMap() {
	allDataFiles, err := os.ReadDir("./Data/")
	if err != nil {
		panic(err)
	}
	index := 0
	for _, file := range allDataFiles {
		if strings.Contains(file.Name(), "WAL") {
			index += 1
		}
	}
	if index == 0 {
		return
	}
	wal.file_num = index
	file, err := os.OpenFile("./Data/WAL"+strconv.Itoa(index)+".db", os.O_RDWR, 0644)
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	defer mmapf.Unmap()
	wal.file_path = "./Data/WAL" + strconv.Itoa(index) + ".db"
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
		velicina_kljuca := binary.BigEndian.Uint64(result[start+21 : start+29])
		velicina_vrednosti := binary.BigEndian.Uint64(result[start+29 : end])
		new_reading_size = int(velicina_kljuca + velicina_vrednosti)
		key := string(result[end : end+int(velicina_kljuca)])
		value := string(result[end+int(velicina_kljuca) : end+int(new_reading_size)])
		start = end + int(new_reading_size)
		end = start + 37
		println("Kljuc:", key)
		println("Vrednost:", value)
		wal.segment_treshold += 1
	}

	file.Close()

}

func ReadLastSegment() []Segment {
	allDataFiles, err := os.ReadDir("./Data/")
	if err != nil {
		panic(err)
	}
	index := 0
	for _, file := range allDataFiles {
		if strings.Contains(file.Name(), "WAL") {
			index += 1
		}
	}
	if index == 0 {
		return nil
	}
	file, err := os.OpenFile("./Data/WAL"+strconv.Itoa(index)+".db", os.O_RDWR, 0644)
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	defer mmapf.Unmap()
	if mmapf == nil {
		return nil
	}
	result := make([]byte, len(mmapf))
	copy(result, mmapf)
	start := 0
	end := 37
	new_reading_size := 0
	segments := make([]Segment, 0)
	for {
		//println(end + new_reading_size)
		//println(mmapf)
		if end+new_reading_size > len(mmapf) {
			break
		}
		crc := result[start : start+4]
		timestamp := result[start+4 : start+20]
		tombstone := result[start+20 : start+21]
		velicina_kljuca := binary.BigEndian.Uint64(result[start+21 : start+29])
		velicina_vrednosti := binary.BigEndian.Uint64(result[start+29 : end])
		new_reading_size = int(velicina_kljuca + velicina_vrednosti)
		key := result[end : end+int(velicina_kljuca)]
		value := result[end+int(velicina_kljuca) : end+int(new_reading_size)]
		start = end + int(new_reading_size)
		segment := Segment{}
		segment.crc = crc
		segment.timestamp = timestamp
		segment.tombstone = tombstone
		segment.keysize = result[start+21 : start+29]
		segment.valuesize = result[start+29 : end]
		segment.key = key
		segment.value = value
		end = start + 37
		segments = append(segments, segment)
	}
	return segments

}

func (wal *WAL) write_to_file_MMap(segment Segment) {
	file, err := os.OpenFile("./Data/WAL"+strconv.Itoa(int(wal.file_num+1))+".db", os.O_RDWR|os.O_CREATE, 0600)
	wal.file_path = "./Data/WAL" + strconv.Itoa(int(wal.file_num+1)) + ".db"
	if isError(err) {
		return
	}
	wal.writeMMap(file, segment)
	wal.file_num += 1
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
	mmapf.Unmap()
	file.Close()
}

func (wal *WAL) deleteOldSegments() {
	allDataFiles, err := os.ReadDir("./Data/")
	if err != nil {
		panic(err)
	}
	fileNames := make([]string, 0)
	for _, file := range allDataFiles {
		if strings.Contains(file.Name(), "WAL") {
			fileNames = append(fileNames, file.Name())
		}
	}
	if len(fileNames) == 0 {
		return
	}
	for index := 0; index < len(fileNames)-1; index++ {
		err = os.Remove("./Data/" + fileNames[index])
		fmt.Println(err)
	}
	err = os.Rename("./Data/"+fileNames[len(fileNames)-1], "./Data/WAL1.db")
	wal.file_path = "./Data/WAL1.db"
	fmt.Println(err)

}

func (wal *WAL) write_to_file(file *os.File) {
	for _, segment := range wal.segments {
		wal.write(file, segment)
	}
	file_1, err := os.OpenFile("./Data/WAL"+strconv.Itoa(int(wal.file_num+1))+".db", os.O_CREATE, 0600)
	if isError(err) {
		return
	}
	file_1.Close()

}

func (wal *WAL) write(file *os.File, segment Segment) {
	err := binary.Write(file, binary.BigEndian, segment.crc)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.timestamp)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.tombstone)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.keysize)
	println(binary.BigEndian.Uint64(segment.keysize))
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.valuesize)
	println(binary.BigEndian.Uint64(segment.valuesize))
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.key)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.value)
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

func WALProba() {
	wal := WAL{}
	wal.Constuct(5, 3)
	//wal.append([]byte("1"), []byte("asdfsdf"), 1)
	//wal.append([]byte("123"), []byte("noicee"), 1)
	//wal.append([]byte("1s"), []byte("asdfsdf1231"), 1)
	//wal.append([]byte("123fd"), []byte("noicee4363"), 1)
	//wal.append([]byte("1dfg"), []byte("asdfsdf6568"), 1)
	//wal.readMMap()
	//wal.deleteOldSegments()
}
