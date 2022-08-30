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
	NumberOfFiles       int
	MerkleElements      []string
	nthElementInSummary int
}

// Setting default parameters for the SSTable
func (sstable *SSTable) Construct() {
	allDataFiles, err := os.ReadDir("./Data/")
	if err != nil {
		panic(err)
	}
	sstable.NumberOfFiles = 0
	sstable.nthElementInSummary = 3
	for _, file := range allDataFiles {
		if strings.Contains(file.Name(), "Data_lvl1") {
			sstable.NumberOfFiles += 1
		}
	}
}

func (sstable *SSTable) Flush(elements []SkipList.Content) {
	sstable.MerkleElements = make([]string, 0, len(elements))
	sstable.CreateFiles(elements)
}

//We use this method to process all the elements from the skip list and make corresponding files

func (sstable *SSTable) CreateFiles(elements []SkipList.Content) {
	file, err := os.OpenFile("./Data/Data_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".db", os.O_CREATE|os.O_APPEND, 0600)
	indexFile, _ := os.OpenFile("./Data/Index_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".db", os.O_CREATE|os.O_APPEND, 0600)
	summaryFile, _ := os.OpenFile("./Data/Summary_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".db", os.O_CREATE|os.O_APPEND, 0600)
	filterFile, _ := os.OpenFile("./Data/BloomFilter_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".db", os.O_CREATE|os.O_RDWR, 0600)
	TOCFile, _ := os.OpenFile("./Data/TOC_lvl1_"+strconv.Itoa(sstable.NumberOfFiles+1)+".txt", os.O_CREATE|os.O_RDWR, 0600)
	bloomFilter := BloomFilter.MakeBloomFilter(10, 0.1)

	stringForTOCFile := "Data file: " + file.Name() + "\nIndex file: " + indexFile.Name() + "\nSummary file: " + summaryFile.Name() + "\nFilter file: " + filterFile.Name() + "\nMerkle file: ./Data/MerkleTree_lvl1_" + strconv.Itoa(sstable.NumberOfFiles+1) + ".db"
	TOCFile.WriteString(stringForTOCFile)
	sstable.NumberOfFiles += 1

	//Writing to file the first and the last key going into the summary File
	sstable.SummaryFileBoundaries(elements, summaryFile)

	if err != nil {
		log.Fatal(err)
	}

	//We add every element from the skiplist into a bloom filter and other files
	for index, element := range elements {
		addToSSTable := false
		if index%sstable.nthElementInSummary == 0 {
			addToSSTable = true
		}
		bloomFilter.Add(element.Key)
		sstable.ElementFlush(element, file, indexFile, summaryFile, addToSSTable)
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
	TOCFile.Close()
	filterFile.Close()
}

// This method packs the element into a desired shape and writes it to the files
func (sstable *SSTable) ElementFlush(element SkipList.Content, file *os.File, indexFile *os.File, summaryFile *os.File, addToSSTable bool) {
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
	sstable.MerkleElements = append(sstable.MerkleElements, string(key))

	//Writing to file

	file.Write(crc)
	file.Write(timestamp)
	file.Write(toombstone)
	file.Write(keySize)
	file.Write(valueSize)
	file.Write(key)
	file.Write(value)

	sstable.InsertIntoIndexFile(key, int64(filePosition), indexFile, summaryFile, addToSSTable)
}

// Insert element into index file
func (sstable *SSTable) InsertIntoIndexFile(key []byte, keyOffset int64, file *os.File, summaryFile *os.File, addToSSTable bool) {
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
	if addToSSTable {
		summaryFile.Write(keySizeB)
		summaryFile.Write(key)
		summaryFile.Write(indexOffsetB)
	}
}

func ReadIndexFile(substr string, indexFileOffset int, desiredKey string) int {
	file, err := os.OpenFile("./Data/Index_lvl"+substr+".db", os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	found := false
	keySize := make([]byte, 8)
	offset := make([]byte, 8)
	file.Seek(int64(indexFileOffset), 0)
	for true {
		_, err = file.Read(keySize)
		if err != nil {
			return -1
		}
		keySizeParsed := binary.BigEndian.Uint64(keySize)
		key := make([]byte, keySizeParsed)
		file.Read(key)
		file.Read(offset)
		if string(key) == desiredKey {
			found = true
			break
		}
	}
	file.Close()
	if !found {
		return -1
	}
	return int(binary.BigEndian.Uint64(offset))
}

func ReadSummaryFile(file *os.File, desiredKey string) int {
	keySize := make([]byte, 8)
	indexFileOffset := make([]byte, 8)
	lastOffset := 0

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
		if string(key) > desiredKey {
			return lastOffset
		}
		if string(key) != desiredKey {
			lastOffset = int(binary.BigEndian.Uint64(indexFileOffset))
			continue
		}
		return int(binary.BigEndian.Uint64(indexFileOffset))
	}
	file.Close()
	return lastOffset
}

func ReadDataFile(fileSubstr string, dataFileOffset int) []byte {
	//fmt.Println("-------------Data File---------------")
	file, err := os.OpenFile("./Data/Data_lvl"+fileSubstr+".db", os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}

	file.Seek(int64(dataFileOffset)+20, 0)

	tombstoneBin := make([]byte, 1)
	file.Read(tombstoneBin)
	if tombstoneBin[0] == 1 {
		fmt.Println("Trazena vrednost je logicki obrisana.")
		file.Close()
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

func GetLvlPosition(fileName string) int {
	for index := len(fileName) - 1; index >= 0; index-- {
		if fileName[index] == '_' {
			return index
		}
	}
	return -1
}

func GetHighestIndex(files []string) (int, string) {
	highestIndex := "0"
	for _, file := range files {
		fileName := file
		if strings.Contains(fileName, "Summary") {
			index := fileName[13:]
			index = strings.Replace(index, ".db", "", -1)
			indexI, _ := strconv.Atoi(index)
			highestIndexI, _ := strconv.Atoi(highestIndex)
			if indexI > highestIndexI {
				highestIndex = index
			}
		}
	}
	if len(files) == 0 {
		return -1, "0"
	}
	position := GetLvlPosition(files[0])
	lvl := files[0][11:position]
	highestIndexI, _ := strconv.Atoi(highestIndex)
	return highestIndexI, lvl
}

func CheckSummaryFiles(files []os.DirEntry, key string) []byte {
	indexFileOffset := -1
	fileSubstr := ""
	dataFileOffset := 0
	filesByLvls := GetFilesByLevels(files)
	found := false
	for _, fileArray := range filesByLvls {
		highestIndex, lvl := GetHighestIndex(fileArray)
		for index := highestIndex; index >= 1; index-- {
			indexFileOffset = KeyInSummaryFile("Summary_lvl"+lvl+"_"+strconv.Itoa(index)+".db", key)
			if indexFileOffset == -1 {
				continue
			} else {
				fileSubstr = lvl + "_" + strconv.Itoa(index)
				dataFileOffset = ReadIndexFile(fileSubstr, indexFileOffset, key)
				if dataFileOffset == -1 {
					continue
				}
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if indexFileOffset == -1 {
		return nil
	}
	return ReadDataFile(fileSubstr, dataFileOffset)
}

func GetFilesByLevels(files []os.DirEntry) [][]string {
	filesByLvls := make([][]string, 0)
	lvl := 1
	filesInALvl := make([]string, 0)
	for _, file := range files {
		fileName := file.Name()
		if strings.Contains(fileName, "Summary") {
			fileLvl := fileName[11:12]
			fLvl, _ := strconv.Atoi(fileLvl)
			if fLvl != lvl {
				filesByLvls = append(filesByLvls, filesInALvl)
				filesInALvl = make([]string, 0)
				lvl = fLvl
			}
			filesInALvl = append(filesInALvl, fileName)
		}
	}
	if len(filesInALvl) != 0 {
		filesByLvls = append(filesByLvls, filesInALvl)
	}
	return filesByLvls
}

func KeyInSummaryFile(fileName string, key string) int {
	summaryFile, err := os.OpenFile("./Data/"+fileName, os.O_RDONLY, 0600)
	if err != nil {
		summaryFile.Close()
		panic(err)
	}
	indexFileOffset := ReadSummaryFile(summaryFile, key)
	if indexFileOffset == -1 {
		summaryFile.Close()
		return -1
	}
	summaryFile.Close()
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
	found := false
	for _, file := range files {
		fileName := file.Name()
		if strings.Contains(fileName, "Filter") {
			bytes, _ := os.ReadFile("./Data/" + fileName)
			newBloom := BloomFilter.BloomFilter{}
			newBloom.Deserialize(bytes)
			if newBloom.Search(key) {
				found = true
				break
			}
		}
	}
	return found
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
