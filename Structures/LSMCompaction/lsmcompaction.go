package LSMCompaction

import (
	"Projekat/Settings"
	"Projekat/Structures/BloomFilter"
	"Projekat/Structures/MerkleTree"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func LSMCompaction(lsmLevel int) {
	settings := Settings.Settings{}
	settings.Path = "./settings.json"
	settings.LoadFromJSON()

	ssTableNames := getSSTableNamesByLevel(lsmLevel)
	if len(ssTableNames) < settings.LsmMaxElementsPerLevel || lsmLevel >= settings.LsmMaxLevels {
		return
	}

	deleteOldFiles(ssTableNames, lsmLevel)
	mergeSSTables(ssTableNames, lsmLevel)

	mergedSSTableName := getSSTableNamesByLevel(lsmLevel)[0]
	availableSerialNumFromNextLSMLevel := getAvailableSerialNumFromNextLSMLevel(lsmLevel)
	newSSTableName := "Data_lvl" + strconv.Itoa(lsmLevel+1) + "_" + availableSerialNumFromNextLSMLevel + ".db"
	_ = os.Rename("./Data/"+mergedSSTableName, "./Data/"+newSSTableName)

	createNewFiles(newSSTableName)

	LSMCompaction(lsmLevel + 1)
}

// mergeSSTables
// Merges SSTables that are passed on as parameters (names of SSTable files)
// Merges all SSTables from same level
// Similar to merge sort algorithm
func mergeSSTables(ssTables []string, lsmLevel int) {
	iterLength := len(ssTables)
	if iterLength%2 == 1 {
		iterLength--
	}
	for i := 0; i < iterLength; i += 2 {
		ssTableFile1, err1 := os.OpenFile("./Data/"+ssTables[i], os.O_RDONLY, 0444)
		if err1 != nil {
			panic(err1)
		}
		ssTableFile2, err2 := os.OpenFile("./Data/"+ssTables[i+1], os.O_RDONLY, 0444)
		if err2 != nil {
			panic(err2)
		}

		newFileSerialNum := getDataFileNameSerialNum(ssTables[i]) + "-" + getDataFileNameSerialNum(ssTables[i+1])
		mergeTwoSSTables(ssTableFile1, ssTableFile2, lsmLevel, newFileSerialNum, iterLength == 2)

		_ = ssTableFile1.Close()
		_ = ssTableFile2.Close()
		err := os.Remove(ssTableFile1.Name())
		if err != nil {
			panic(err)
		}
		err = os.Remove(ssTableFile2.Name())
		if err != nil {
			panic(err)
		}
	}

	if iterLength >= 2 {
		mergeSSTables(getSSTableNamesByLevel(lsmLevel), lsmLevel)
	}
}

// mergeTwoSSTables
// doDelete is an indicator to do physical removal of items
// to ensure the item is removed, removal needs to be applicable only when merging the last two SSTables
// if an item gets removed before that, there is a chance there will be an older version of the item
// that has tombstone = 0 which managed to skip on merging with the file where that item has tombstone != 0
// in which case the item will still be in the final merged SSTable
func mergeTwoSSTables(ssTableFile1, ssTableFile2 *os.File, lsmLevel int, newFileSerialNum string, doDelete bool) {
	newSSTableFile, err := os.Create("./Data/Data_lvl" + strconv.Itoa(lsmLevel) + "_" + newFileSerialNum + ".db")
	if err != nil {
		panic(err)
	}

	ssTableElement1, err1 := getNextSSTableElement(ssTableFile1)
	ssTableElement2, err2 := getNextSSTableElement(ssTableFile2)
	for {
		if err1 == io.EOF && err2 == io.EOF {
			break
		}
		if err1 == io.EOF {
			if ssTableElement2.Tombstone[0] == 0 || !doDelete {
				_, _ = newSSTableFile.Write(ssTableElement2.GetAsByteArray())
			}
			ssTableElement2, err2 = getNextSSTableElement(ssTableFile2)
			continue
		}
		if err2 == io.EOF {
			if ssTableElement1.Tombstone[0] == 0 || !doDelete {
				_, _ = newSSTableFile.Write(ssTableElement1.GetAsByteArray())
			}
			ssTableElement1, err1 = getNextSSTableElement(ssTableFile1)
			continue
		}

		if ssTableElement1.GetKey() < ssTableElement2.GetKey() {
			if ssTableElement1.Tombstone[0] == 0 || !doDelete {
				_, _ = newSSTableFile.Write(ssTableElement1.GetAsByteArray())
			}
			ssTableElement1, err1 = getNextSSTableElement(ssTableFile1)
		} else if ssTableElement1.GetKey() > ssTableElement2.GetKey() {
			if ssTableElement2.Tombstone[0] == 0 || !doDelete {
				_, _ = newSSTableFile.Write(ssTableElement2.GetAsByteArray())
			}
			ssTableElement2, err2 = getNextSSTableElement(ssTableFile2)
		} else {
			if ssTableElement1.CheckNewer(ssTableElement2) {
				if ssTableElement1.Tombstone[0] == 0 || !doDelete {
					_, _ = newSSTableFile.Write(ssTableElement1.GetAsByteArray())
				}
			} else {
				if ssTableElement2.Tombstone[0] == 0 || !doDelete {
					_, _ = newSSTableFile.Write(ssTableElement2.GetAsByteArray())
				}
			}
			ssTableElement1, err1 = getNextSSTableElement(ssTableFile1)
			ssTableElement2, err2 = getNextSSTableElement(ssTableFile2)
		}
	}

	_ = newSSTableFile.Close()
}

func getNextSSTableElement(ssTableFile *os.File) (SSTableElement, error) {
	ssTableElBytes := make([]byte, 37)
	_, err := ssTableFile.Read(ssTableElBytes)
	if err != nil {
		if err == io.EOF {
			return SSTableElement{}, err
		} else {
			panic(err)
		}
	}
	keySize := binary.BigEndian.Uint64(ssTableElBytes[21:29])
	valueSize := binary.BigEndian.Uint64(ssTableElBytes[29:37])

	offset := 37 + keySize + valueSize
	ssTableElBytes = make([]byte, offset)
	_, _ = ssTableFile.Seek(-37, 1)
	_, err = ssTableFile.Read(ssTableElBytes)
	if err != nil {
		panic(err)
	}

	return createSSTableElement(ssTableElBytes), nil
}

func getSSTableNamesByLevel(lsmLevel int) []string {
	allDataFiles, err := ioutil.ReadDir("./Data/")
	if err != nil {
		panic(err)
	}
	ssTables := make([]string, 0)
	for _, file := range allDataFiles {
		if strings.Contains(file.Name(), "Data_lvl"+strconv.Itoa(lsmLevel)) {
			ssTables = append(ssTables, file.Name())
		}
	}

	return ssTables
}

func createSSTableElement(data []byte) SSTableElement {
	ssTableElement := SSTableElement{}

	var crc [4]byte
	for i, b := range data[:4] {
		crc[i] = b
	}
	ssTableElement.CRC = crc

	var timestamp [16]byte
	for i, b := range data[4:20] {
		timestamp[i] = b
	}
	ssTableElement.Timestamp = timestamp

	var tombstone [1]byte
	for i, b := range data[20:21] {
		tombstone[i] = b
	}
	ssTableElement.Tombstone = tombstone

	var keySize [8]byte
	for i, b := range data[21:29] {
		keySize[i] = b
	}
	ssTableElement.KeySize = keySize

	var valueSize [8]byte
	for i, b := range data[29:37] {
		valueSize[i] = b
	}
	ssTableElement.ValueSize = valueSize

	ssTableElement.Key = data[37 : 37+ssTableElement.GetKeySize()]
	ssTableElement.Value = data[37+ssTableElement.GetKeySize():]
	return ssTableElement
}

// getDataFileNameSerialNum example: if name is "Data_Data_lvl1_2.db" returns 2
func getDataFileNameSerialNum(ssTableName string) string {
	splitByUnderscore := strings.Split(ssTableName, "_")
	serialNum := splitByUnderscore[2]
	serialNum = strings.ReplaceAll(serialNum, ".db", "")
	return serialNum
}

func getAvailableSerialNumFromNextLSMLevel(lsmLevel int) string {
	ssTablesFromNextLevel := getSSTableNamesByLevel(lsmLevel + 1)
	return strconv.Itoa(len(ssTablesFromNextLevel) + 1)
}

func deleteOldFiles(ssTableNames []string, lsmLevel int) {
	for _, ssTableName := range ssTableNames {
		err := os.Remove("./Data/MerkleTree_lvl" + strconv.Itoa(lsmLevel) + "_" + getDataFileNameSerialNum(ssTableName) + ".db")
		if err != nil {
			panic(err)
		}
		err = os.Remove("./Data/BloomFilter_lvl" + strconv.Itoa(lsmLevel) + "_" + getDataFileNameSerialNum(ssTableName) + ".db")
		if err != nil {
			panic(err)
		}
		err = os.Remove("./Data/Index_lvl" + strconv.Itoa(lsmLevel) + "_" + getDataFileNameSerialNum(ssTableName) + ".db")
		if err != nil {
			panic(err)
		}
		err = os.Remove("./Data/Summary_lvl" + strconv.Itoa(lsmLevel) + "_" + getDataFileNameSerialNum(ssTableName) + ".db")
		if err != nil {
			panic(err)
		}
	}
}

func createNewFiles(ssTableName string) {
	ssTableFile, err := os.OpenFile("./Data/"+ssTableName, os.O_RDONLY, 0444)
	if err != nil {
		panic(err)
	}

	// for every index i in ssTableKeys, its offset is in ssTableElementPositions[i]
	ssTableKeys := make([]string, 0)
	ssTableElementPositions := make([]uint64, 0)
	for {
		position, err := ssTableFile.Seek(0, 1)
		ssTableElement, err := getNextSSTableElement(ssTableFile)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		ssTableKeys = append(ssTableKeys, ssTableElement.GetKey())
		ssTableElementPositions = append(ssTableElementPositions, uint64(position))
	}
	ssTableNameSplitUnderscore := strings.Split(ssTableName, "_")

	// creating new Merkle tree
	merkleTree := MerkleTree.MerkleTree{}
	merkleTree.Form(ssTableKeys)
	merkleTree.Serialize("./Data/MerkleTree_" + strings.Join(ssTableNameSplitUnderscore[1:], "_"))

	// creating new Bloom filer
	newBloomFilter := BloomFilter.MakeBloomFilter(len(ssTableKeys), 0.1)
	for _, key := range ssTableKeys {
		newBloomFilter.Add(key)
	}

	newBloomFilterFile, err := os.Create("./Data/BloomFilter_" + strings.Join(ssTableNameSplitUnderscore[1:], "_"))
	if err != nil {
		panic(err)
	}
	_, _ = newBloomFilterFile.Write(newBloomFilter.Serialize())
	_ = newBloomFilterFile.Close()

	// creating index and summary files
	indexFile, err := os.Create("./Data/Index_" + strings.Join(ssTableNameSplitUnderscore[1:], "_"))
	if err != nil {
		panic(err)
	}
	summaryFile, err := os.Create("./Data/Summary_" + strings.Join(ssTableNameSplitUnderscore[1:], "_"))
	if err != nil {
		panic(err)
	}

	// writing first summary file element
	firstElementKeySizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstElementKeySizeBytes, uint64(len(ssTableKeys[0])))
	_, _ = summaryFile.Write(firstElementKeySizeBytes)
	_, _ = summaryFile.Write([]byte(ssTableKeys[0]))

	// writing last summary file element
	lastElementKeySizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lastElementKeySizeBytes, uint64(len(ssTableKeys[len(ssTableKeys)-1])))
	_, _ = summaryFile.Write(lastElementKeySizeBytes)
	_, _ = summaryFile.Write([]byte(ssTableKeys[len(ssTableKeys)-1]))

	for i := 0; i < len(ssTableKeys); i++ {
		indexOffset, err := indexFile.Seek(0, 1)
		if err != nil {
			panic(err)
		}
		keyOffsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyOffsetBytes, ssTableElementPositions[i])

		indexOffsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(indexOffsetBytes, uint64(indexOffset))

		keySizeBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keySizeBytes, uint64(len(ssTableKeys[i])))

		_, _ = indexFile.Write(keySizeBytes)
		_, _ = indexFile.Write([]byte(ssTableKeys[i]))
		_, _ = indexFile.Write(keyOffsetBytes)
		if i%3 == 0 {
			_, _ = summaryFile.Write(keySizeBytes)
			_, _ = summaryFile.Write([]byte(ssTableKeys[i]))
			_, _ = summaryFile.Write(indexOffsetBytes)
		}
	}
	_ = ssTableFile.Close()
	_ = indexFile.Close()
	_ = summaryFile.Close()
}
