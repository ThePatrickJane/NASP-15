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
   Tombstone = If this record was deleted and has a Value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type Segment struct {
	CRC       []byte
	TimeStamp []byte
	TombStone []byte
	KeySize   []byte
	ValueSize []byte
	Key       []byte
	Value     []byte
}

// Konstruktor za segment
func (segment *Segment) Construct(key []byte, value []byte, tombstone uint8) {
	segment.Key = key
	segment.Value = value
	segment.CRC = make([]byte, 4)
	binary.BigEndian.PutUint32(segment.CRC, uint32(CRC32(key)))
	segment.TimeStamp = make([]byte, 16)
	binary.BigEndian.PutUint64(segment.TimeStamp, uint64(time.Now().Unix()))
	segment.TombStone = make([]byte, 1)
	segment.TombStone[0] = byte(tombstone)
	segment.KeySize = make([]byte, 8)
	binary.BigEndian.PutUint64(segment.KeySize, uint64(len(key)))
	segment.ValueSize = make([]byte, 8)
	binary.BigEndian.PutUint64(segment.ValueSize, uint64(len(value)))
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
	wal.file_num = 1
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
func (wal *WAL) Insert(key []byte, value []byte, tombstone uint8) {
	segment := Segment{}
	segment.Construct(key, value, tombstone)
	//println(binary.Size(segment))
	wal.treshold += 1
	wal.segment_treshold += 1
	wal.segments = append(wal.segments, segment)

	file, err := os.OpenFile(wal.file_path, os.O_RDWR|os.O_CREATE, 0600)
	if isError(err) {
		file.Close()
		return
	}
	wal.writeMMap(file, segment)
	file.Close()
	if int(wal.segment_treshold) == wal.maxElementsInSegment {
		file, err = os.OpenFile("./Data/WAL"+strconv.Itoa(int(wal.file_num+1))+".db", os.O_CREATE, 0600)
		wal.file_path = "./Data/WAL" + strconv.Itoa(int(wal.file_num+1)) + ".db"
		wal.segments = make([]Segment, 0)
		wal.segment_treshold = 0
		wal.file_num += 1
		file.Close()
	}
	if wal.file_num > wal.maxSegments {
		wal.deleteOldSegments()
	}
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
	wal.file_path = "./Data/WAL" + strconv.Itoa(index) + ".db"
	if mmapf == nil {
		mmapf.Unmap()
		file.Close()
		return
	}
	result := make([]byte, len(mmapf))
	copy(result, mmapf)
	start := 0
	end := 37
	new_reading_size := 0
	for {
		//println(end + new_reading_size)
		//println(mmapf)
		if end+new_reading_size > len(mmapf) {
			break
		}
		velicina_kljuca := binary.BigEndian.Uint64(result[start+21 : start+29])
		velicina_vrednosti := binary.BigEndian.Uint64(result[start+29 : end])
		new_reading_size = int(velicina_kljuca + velicina_vrednosti)
		//Key := string(result[end : end+int(velicina_kljuca)])
		//Value := string(result[end+int(velicina_kljuca) : end+int(new_reading_size)])
		start = end + int(new_reading_size)
		end = start + 37
		//println("Kljuc:", Key)
		//println("Vrednost:", Value)
		wal.segment_treshold += 1
	}
	mmapf.Unmap()
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
	if mmapf == nil {
		mmapf.Unmap()
		file.Close()
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
		segment := Segment{}
		segment.CRC = crc
		segment.TimeStamp = timestamp
		segment.TombStone = tombstone
		segment.KeySize = result[start+21 : start+29]
		segment.ValueSize = result[start+29 : end]
		segment.Key = key
		segment.Value = value
		start = end + int(new_reading_size)
		end = start + 37
		segments = append(segments, segment)
	}
	mmapf.Unmap()
	file.Close()
	return segments

}

func (wal *WAL) writeMMap(file *os.File, segment Segment) {
	currentLen, err := fileLen(file)
	sz := 0
	if isError(err) {
		return
	}
	sz += binary.Size(segment.CRC)
	sz += binary.Size(segment.TimeStamp)
	sz += binary.Size(segment.TombStone)
	sz += binary.Size(segment.KeySize)
	sz += binary.Size(segment.ValueSize)
	sz += binary.Size(segment.Key)
	sz += binary.Size(segment.Value)

	err = file.Truncate(currentLen + int64(sz))
	if isError(err) {
		return
	}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.CRC))], segment.CRC)
	currentLen += int64(binary.Size(segment.CRC))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.TimeStamp))], segment.TimeStamp)
	currentLen += int64(binary.Size(segment.TimeStamp))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.TombStone))], segment.TombStone)
	currentLen += int64(binary.Size(segment.TombStone))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.KeySize))], segment.KeySize)
	currentLen += int64(binary.Size(segment.KeySize))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.ValueSize))], segment.ValueSize)
	currentLen += int64(binary.Size(segment.ValueSize))
	copy(mmapf[currentLen:currentLen+int64(binary.Size(segment.Key))], segment.Key)
	currentLen += int64(binary.Size(segment.Key))
	copy(mmapf[currentLen:], segment.Value)
	currentLen += int64(binary.Size(segment.Value))
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
		//fmt.Println(err)
	}
	err = os.Rename("./Data/"+fileNames[len(fileNames)-1], "./Data/WAL1.db")
	wal.file_path = "./Data/WAL1.db"
	wal.file_num = 1
	//fmt.Println(err)

}

func (wal *WAL) write_to_file(file *os.File) {
	for _, segment := range wal.segments {
		wal.write(file, segment)
	}
	file_1, err := os.OpenFile("./Data/WAL"+strconv.Itoa(int(wal.file_num+1))+".db", os.O_CREATE, 0600)
	if isError(err) {
		file_1.Close()
		return
	}
	file_1.Close()

}

func (wal *WAL) write(file *os.File, segment Segment) {
	err := binary.Write(file, binary.BigEndian, segment.CRC)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.TimeStamp)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.TombStone)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.KeySize)
	println(binary.BigEndian.Uint64(segment.KeySize))
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.ValueSize)
	println(binary.BigEndian.Uint64(segment.ValueSize))
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.Key)
	if err != nil {
		log.Fatal("Write failed")
	}

	err = binary.Write(file, binary.BigEndian, segment.Value)
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
	wal.Insert([]byte("1"), []byte("asdfsdf"), 1)
	wal.Insert([]byte("123"), []byte("noicee"), 1)
	wal.Insert([]byte("1s"), []byte("asdfsdf1231"), 1)
	wal.Insert([]byte("123fd"), []byte("noicee4363"), 1)
	wal.Insert([]byte("1dfg"), []byte("asdfsdf6568"), 1)
	wal.Insert([]byte("1dfg"), []byte("asdfsdf6568"), 1)
	//wal.readMMap()
	//wal.deleteOldSegments()
}
