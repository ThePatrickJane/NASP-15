package SSTable

import (
	"Projekat/Structures/BloomFilter"
	"Projekat/Structures/MerkleTree"
	"Projekat/Structures/SkipList"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	FILE_PATH = "Data/SSTable"
)

type SSTable struct {
	NumberOfFiles  int
	MerkleElements [][]byte
}

// Setting default parameters for the SSTable
func (sstable *SSTable) Construct() {
	allDataFiles, err := os.ReadDir("./Data/")
	if err != nil {
		panic(err)
	}
	sstable.NumberOfFiles = 0
	for _, file := range allDataFiles {
		if strings.Contains(file.Name(), "Data_Data_lvl1") {
			sstable.NumberOfFiles += 1
		}
	}
}

func (sstable *SSTable) Flush(elements []SkipList.Content) {
	sstable.MerkleElements = make([][]byte, 0, len(elements))
	sstable.CreateFiles(elements)
}

//We use this method to process all the elements from the skip list and make corresponding files

func (sstable *SSTable) CreateFiles(elements []SkipList.Content) {
	file, err := os.OpenFile("./Data/Data_Data_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".db", os.O_CREATE|os.O_APPEND, 0600)
	indexFile, _ := os.OpenFile("./Data/Index_Data_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".db", os.O_CREATE|os.O_APPEND, 0600)
	summaryFile, _ := os.OpenFile("./Data/Summary_Data_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".db", os.O_CREATE|os.O_APPEND, 0600)
	filterFile, _ := os.OpenFile("./Data/Filter_Data_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".db", os.O_CREATE|os.O_RDWR, 0600)

	bloomFilter := BloomFilter.MakeBloomFilter(10, 0.1)

	sstable.NumberOfFiles += 1

	//Writing to file the first and the last key going into the summary File
	sstable.SummaryFileBoundaries(elements, summaryFile)

	if err != nil {
		log.Fatal(err)
	}

	//We add every element from the skiplist into a bloom filter and other files
	for _, element := range elements {
		bloomFilter.Add(element.Key)
		sstable.ElementFlush(element, file, indexFile, summaryFile)
	}

	//Creation and serialization of merkle tree
	merkleTree := MerkleTree.MerkleTree{}
	merkleTree.Form(sstable.MerkleElements)
	merkleTree.Serialize("./Data/MerkleTree_lvl1_" + strconv.Itoa(sstable.NumberOfFiles) + ".db")

	//Bloom filter serialization
	bloomSerijalizovan := bloomFilter.Serialize()

	//Writing serialized bloom filter to file
	filterFile.Write(bloomSerijalizovan)

	file.Close()
	indexFile.Close()
	summaryFile.Close()
}

// This method packs the element into a desired shape and writes it to the files
func (sstable *SSTable) ElementFlush(element SkipList.Content, file *os.File, indexFile *os.File, summaryFile *os.File) {
	//Defining needed variables

	key := []byte(element.Key)
	value := element.Value
	crc := make([]byte, 4)
	timestamp := make([]byte, 16)
	toombstone := make([]byte, 1)
	keySize := make([]byte, 8)
	valueSize := make([]byte, 8)
	filePosition, _ := file.Seek(0, 1)

	//Converting

	binary.BigEndian.PutUint32(crc, CRC32(key))
	binary.BigEndian.PutUint64(timestamp, uint64(element.Timestamp))
	if element.Tombstone == true {
		toombstone[0] = 1
	} else {
		toombstone[0] = 0
	}
	binary.BigEndian.PutUint64(keySize, uint64(len(key)))
	binary.BigEndian.PutUint64(valueSize, uint64(len(value)))

	segment := make([]byte, 0)
	segment = append(segment, crc...)
	segment = append(segment, timestamp...)
	segment = append(segment, toombstone...)
	segment = append(segment, keySize...)
	segment = append(segment, valueSize...)
	segment = append(segment, key...)
	segment = append(segment, value...)
	sstable.MerkleElements = append(sstable.MerkleElements, segment)

	//Writing to file

	file.Write(crc)
	file.Write(timestamp)
	file.Write(toombstone)
	file.Write(keySize)
	file.Write(valueSize)
	file.Write(key)
	file.Write(value)

	sstable.InsertIntoIndexFile(key, int64(filePosition), indexFile, summaryFile)
}

// Insert element into index file
func (sstable *SSTable) InsertIntoIndexFile(key []byte, keyOffset int64, file *os.File, summaryFile *os.File) {
	keySizeB := make([]byte, 8)
	keyOffsetB := make([]byte, 8)
	indexOffsetB := make([]byte, 8)
	indexOffset, _ := file.Seek(0, 1)

	binary.BigEndian.PutUint64(keySizeB, uint64(len(key)))
	binary.BigEndian.PutUint64(keyOffsetB, uint64(keyOffset))
	binary.BigEndian.PutUint64(indexOffsetB, uint64(indexOffset))

	file.Write(keySizeB)
	file.Write(key)
	file.Write(keyOffsetB)
	summaryFile.Write(keySizeB)
	summaryFile.Write(key)
	summaryFile.Write(indexOffsetB)
}

func ReadIndexFile(substr string, indexFileOffset int) int {
	file, err := os.OpenFile("./Data/Index_Data_lvl"+substr+".db", os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	keySize := make([]byte, 8)
	offset := make([]byte, 8)
	file.Seek(int64(indexFileOffset), 0)

	file.Read(keySize)
	keySizeParsed := binary.BigEndian.Uint64(keySize)
	key := make([]byte, keySizeParsed)
	file.Read(key)
	file.Read(offset)

	return int(binary.BigEndian.Uint64(offset))
}

func ReadSummaryFile(file *os.File, desiredKey string) int {
	keySize := make([]byte, 8)
	indexFileOffset := make([]byte, 8)

	firstKey, lastKey := ReadSummaryFileLimits(file)
	if desiredKey >= firstKey && desiredKey <= lastKey {
		//fmt.Println("Status: Found")
	} else {
		//fmt.Println("Status: Not found")
		return -1
	}

	for true {
		_, err := file.Read(keySize)
		if err != nil {
			break
		}
		keySizeParsed := binary.BigEndian.Uint64(keySize)
		key := make([]byte, keySizeParsed)
		file.Read(key)
		file.Read(indexFileOffset)
		if string(key) != desiredKey {
			continue
		}
		return int(binary.BigEndian.Uint64(indexFileOffset))
	}
	file.Close()
	return -1
}

func ReadDataFile(fileSubstr string, dataFileOffset int) []byte {
	//fmt.Println("-------------Data File---------------")
	file, err := os.OpenFile("./Data/Data_Data_lvl"+fileSubstr+".db", os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}

	file.Seek(int64(dataFileOffset)+20, 0)

	tombstoneBin := make([]byte, 1)
	file.Read(tombstoneBin)
	if tombstoneBin[0] == 1 {
		fmt.Println("Trazena vrednost je logicki obrisana.")
		return nil
	}

	keySize := make([]byte, 8)
	valueSize := make([]byte, 8)

	file.Read(keySize)
	file.Read(valueSize)

	keySizeParsed := binary.BigEndian.Uint64(keySize)
	key := make([]byte, keySizeParsed)
	file.Read(key)
	//fmt.Println(string(key))

	valueSizeParsed := binary.BigEndian.Uint64(valueSize)
	value := make([]byte, valueSizeParsed)
	file.Read(value)
	//fmt.Println(string(value))
	file.Close()
	//fmt.Println("-------------------------------------")
	return value
}

// Write into the summary file the first and last key in the skip list
func (sstable *SSTable) SummaryFileBoundaries(elements []SkipList.Content, summaryFile *os.File) {
	firstKey := []byte(elements[0].Key)
	lastKey := []byte(elements[len(elements)-1].Key)

	firstKeySize := make([]byte, 8)
	lastKeySize := make([]byte, 8)

	binary.BigEndian.PutUint64(firstKeySize, uint64(len(firstKey)))
	binary.BigEndian.PutUint64(lastKeySize, uint64(len(lastKey)))

	summaryFile.Write(firstKeySize)
	summaryFile.Write(firstKey)
	summaryFile.Write(lastKeySize)
	summaryFile.Write(lastKey)
}

func GetAllFiles() []os.DirEntry {
	allFiles, err := os.ReadDir("./Data/")
	if err != nil {
		panic(err)
	}
	return allFiles
}

func CheckSummaryFiles(files []os.DirEntry, key string) []byte {
	indexFileOffset := -1
	fileSubstr := ""
	for _, file := range files {
		fileName := file.Name()
		if strings.Contains(fileName, "Summary") {
			indexFileOffset = KeyInSummaryFile(file.Name(), key)
			if indexFileOffset == -1 {
				continue
			} else {
				fileSubstr = fileName[16:]
				fileSubstr = strings.Replace(fileSubstr, ".db", "", -1)
				break
			}
		}
	}
	if indexFileOffset == -1 {
		return nil
	}

	dataFileOffset := ReadIndexFile(fileSubstr, indexFileOffset)
	return ReadDataFile(fileSubstr, dataFileOffset)
}

func KeyInSummaryFile(fileName string, key string) int {
	summaryFile, err := os.OpenFile("./Data/"+fileName, os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}
	indexFileOffset := ReadSummaryFile(summaryFile, key)
	if indexFileOffset == -1 {
		return -1
	}
	return indexFileOffset
}

func Find(key string) []byte {
	allFiles := GetAllFiles()
	if !CheckBloomFilter(allFiles, key) {
		//fmt.Println("Status: Not Found")
		return nil
	}
	return CheckSummaryFiles(allFiles, key)
}

func CheckBloomFilter(files []os.DirEntry, key string) bool {
	for _, file := range files {
		fileName := file.Name()
		if strings.Contains(fileName, "Filter") {
			bytes, _ := os.ReadFile("./Data/" + fileName)
			newBloom := BloomFilter.BloomFilter{}
			newBloom.Deserialize(bytes)
			return newBloom.Search(key)
		}
	}
	return false
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func ReadSummaryFileLimits(summaryFile *os.File) (string, string) {
	firstKeySize := make([]byte, 8)
	lastKeySize := make([]byte, 8)

	summaryFile.Read(firstKeySize)
	firstKeySizeParsed := binary.BigEndian.Uint64(firstKeySize)
	firstKey := make([]byte, firstKeySizeParsed)
	summaryFile.Read(firstKey)
	//fmt.Println("-------------Key limits--------------")
	//fmt.Println("First key: " + string(firstKey))

	summaryFile.Read(lastKeySize)
	lastKeySizeParsed := binary.BigEndian.Uint64(lastKeySize)
	lastKey := make([]byte, lastKeySizeParsed)
	summaryFile.Read(lastKey)
	//fmt.Println("Last key: " + string(lastKey))
	//fmt.Println("-------------------------------------")

	return string(firstKey), string(lastKey)
}

//Reads the whole IndexFile
//func ReadIndexFile(index int) {
//	file, err := os.OpenFile("./Data/SSTable_Index_Data_lvl1_"+string(index)+".db", os.O_RDONLY, 0600)
//	if err != nil {
//		log.Fatal(err)
//	}
//	keySize := make([]byte, 8)
//	offset := make([]byte, 8)
//	for true {
//		_, err := file.Read(keySize)
//		if err != nil {
//			break
//		}
//		keySizeParsed := binary.BigEndian.Uint64(keySize)
//		key := make([]byte, keySizeParsed)
//		file.Read(key)
//		fmt.Println(string(key))
//
//		file.Read(offset)
//		fmt.Println(binary.BigEndian.Uint64(offset))
//	}
//	file.Close()
//}

//func (sstable *SSTable) ReadDataFile() {
//	file, err := os.OpenFile("./Data/SSTable_Data_lvl1_"+strconv.Itoa(sstable.NumberOfDataFiles)+".db", os.O_RDWR|os.O_CREATE, 0600)
//	if err != nil {
//		log.Fatal(err)
//	}
//	keySize := make([]byte, 8)
//	valueSize := make([]byte, 8)
//	for true {
//		file.Seek(21, 1)
//		_, err := file.Read(keySize)
//		if err != nil {
//			break
//		}
//		file.Read(valueSize)
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		keySizeParsed := binary.BigEndian.Uint64(keySize)
//		key := make([]byte, keySizeParsed)
//		file.Read(key)
//		fmt.Println(string(key))
//
//		valueSizeParsed := binary.BigEndian.Uint64(valueSize)
//		value := make([]byte, valueSizeParsed)
//		file.Read(value)
//		fmt.Println(string(value))
//	}
//	file.Close()
//}
