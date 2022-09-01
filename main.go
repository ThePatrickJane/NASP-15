package main

import (
	"Projekat/Structures/CountMinSketch"
	"Projekat/Structures/HyperLogLog"
	"Projekat/Structures/KVEngine"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func mainMenu(kvengine *KVEngine.KVEngine) {
	for {
		fmt.Println("Izaberite opciju: GET - 1, PUT - 2, DELETE - 3, COMPACTION - 4, exit - 0")
		fmt.Print("Opcija: ")
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		text = strings.Replace(text, "\r", "", -1)

		if strings.Compare(text, "1") == 0 {
			getMenu(kvengine)
		} else if strings.Compare(text, "2") == 0 {
			putMenu(kvengine)
		} else if strings.Compare(text, "3") == 0 {
			deleteMenu(kvengine)
		} else if strings.Compare(text, "4") == 0 {
			kvengine.Compactions()
			fmt.Println("Ivrsena kompakcija.")
		} else if strings.Compare(text, "0") == 0 {
			break
		} else {
			fmt.Println("Nepostojeca opcija.")
		}
	}
}

func getMenu(kvengine *KVEngine.KVEngine) {
	fmt.Print("Unesite kljuc: ")
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.Replace(key, "\n", "", -1)
	key = strings.Replace(key, "\r", "", -1)

	for {
		fmt.Println("Izaberite opciju: GET STRING - 1, GET HYPERLOGLOG - 2, GET COUNTMINSKETCH - 3, exit - 0")
		fmt.Print("Opcija: ")

		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		text = strings.Replace(text, "\r", "", -1)

		found, data := kvengine.Get(key)

		if !found {
			fmt.Println("Neuspelo trazenje.")
			break
		}

		if strings.Compare(text, "1") == 0 {

			value := string(data)
			fmt.Println("Pod zadatim kljucem se nalazi vrednost: ", value)
			break

		} else if strings.Compare(text, "2") == 0 {

			hll := HyperLogLog.HyperLogLog{}
			hll.Deserialize(data)
			fmt.Println("Pod zadatim kljucem se nalazi vrednost (HLL): ")
			fmt.Println(hll)
			break

		} else if strings.Compare(text, "3") == 0 {

			cms := CountMinSketch.CountMinSketch{}
			cms.Deserialize(data)
			fmt.Println("Pod zadatim kljucem se nalazi vrednost (CMS): ")
			fmt.Println(cms)
			break

		} else if strings.Compare(text, "0") == 0 {
			break
		} else {
			fmt.Println("Nepostojeca opcija.")
		}
	}
}

func putMenu(kvengine *KVEngine.KVEngine) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Unesite kljuc: ")
	key, _ := reader.ReadString('\n')
	key = strings.Replace(key, "\n", "", -1)
	key = strings.Replace(key, "\r", "", -1)

	for {
		fmt.Println("Izaberite opciju: PUT STRING - 1, PUT HYPERLOGLOG - 2, PUT COUNTMINSKETCH - 3, exit - 0")
		fmt.Print("Opcija: ")

		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		text = strings.Replace(text, "\r", "", -1)

		// u if-ovima se za string samo pozove dodavanje u bazu, a za druge dve strukture se naprave nove instance, dodaju se neki podaci u strukture, odstampaju se, serijalizuju i dodaju u bazu

		if strings.Compare(text, "1") == 0 {

			fmt.Print("Unesite vrednost: ")
			value, _ := reader.ReadString('\n')
			value = strings.Replace(value, "\n", "", -1)
			value = strings.Replace(value, "\r", "", -1)
			data := []byte(value)

			inserted := kvengine.Put(key, data)

			if inserted {
				fmt.Println("Uspesno je dodata vrednost pod zadatim kljucem.")
			} else {
				fmt.Println("Neuspesno dodavanje.")
			}

			break

		} else if strings.Compare(text, "2") == 0 {

			hll := HyperLogLog.GetTestHLL()
			data := hll.Serialize()

			inserted := kvengine.Put(key, data)

			if inserted {
				fmt.Println("Uspesno je dodata HLL vrednost pod zadatim kljucem.")
				fmt.Println("HLL: ", hll)
			} else {
				fmt.Println("Neuspesno dodavanje.")
			}

			break

		} else if strings.Compare(text, "3") == 0 {

			cms := CountMinSketch.GetTestCMS()
			data := cms.Serialize()

			inserted := kvengine.Put(key, data)

			if inserted {
				fmt.Println("Uspesno je dodata CMS vrednost pod zadatim kljucem.")
				fmt.Println("CMS: ", cms)
			} else {
				fmt.Println("Neuspesno dodavanje.")
			}

			break

		} else if strings.Compare(text, "0") == 0 {
			break
		} else {
			fmt.Println("Nepostojeca opcija.")
		}
	}
}

func deleteMenu(kvengine *KVEngine.KVEngine) {
	fmt.Print("Unesite kljuc: ")
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.Replace(key, "\n", "", -1)
	key = strings.Replace(key, "\r", "", -1)

	deleted := kvengine.Delete(key)

	if deleted {
		fmt.Println("Uspesno je izbrisana vrednost pod zadatim kljucem.")
	} else {
		fmt.Println("Vrednost pod zadatim kljucem nije pronadjena.")
	}
}

func main() {
	//for i := 1; i < 31; i++ {
	//	kvengine.Delete("proba" + strconv.Itoa(i))
	//}
	//for i := 1; i < 31; i++ {
	//	_, data := kvengine.Get("proba" + strconv.Itoa(i))
	//	fmt.Println(string(data))
	//}
	//for i := 10; i < 16; i++ {
	//	kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	//}
	//for i := 1; i < 31; i++ {
	//	_, data := kvengine.Get("proba" + strconv.Itoa(i))
	//	fmt.Println(string(data))
	//}
	//kvengine.Compactions() // puca kod ovih kompakcija, remove ./Data/Data_lvl1_10.db: The process cannot access the file because it is being used by another process.
	//for i := 1; i < 31; i++ {
	//	_, data := kvengine.Get("proba" + strconv.Itoa(i))
	//	fmt.Println(string(data))
	//}

	kvengine := KVEngine.MakeKVEngine()
	for i := 1; i < 4; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	for i := 4; i < 7; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	for i := 7; i < 10; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	kvengine.Compactions()
	for i := 10; i < 13; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	for i := 13; i < 16; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	for i := 16; i < 19; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	kvengine.Compactions()
	for i := 19; i < 22; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	for i := 22; i < 25; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	for i := 25; i < 28; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}
	kvengine.Compactions() // na drugom nivou ima 3 sstabele
	for i := 1; i < 4; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	for i := 4; i < 7; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	for i := 7; i < 10; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	kvengine.Compactions()
	for i := 10; i < 13; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	for i := 13; i < 16; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	for i := 16; i < 19; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	kvengine.Compactions()
	for i := 19; i < 22; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	for i := 22; i < 25; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	for i := 25; i < 28; i++ {
		kvengine.Put("proba"+strconv.Itoa(i), []byte(strconv.Itoa(i + 100)))
	}
	kvengine.Compactions()
	mainMenu(&kvengine)
	//Wal.WALProba()
	//sstable := SSTable.SSTable{}
	//sstable.Construct()
	//mem := Memtable.New(5, 5)
	//kvengine.Put("proba", []byte("0"))
	//kvengine.Put("proba1", []byte("1"))
	//kvengine.Put("proba2", []byte("2"))
	//kvengine.Put("proba3", []byte("3"))
	//kvengine.Put("proba4", []byte("4"))
	//kvengine.Put("proba5", []byte("5"))
	//kvengine.Put("proba6", []byte("6"))
	//kvengine.Put("proba7", []byte("7"))
	//kvengine.Put("proba8", []byte("8"))
	//kvengine.Put("proba9", []byte("9"))
	//kvengine.Put("probaa", []byte("a"))
	//kvengine.Put("probab", []byte("b"))
	//mem.BrziAdd("proba3", []byte("123"))
	//mem.BrziAdd("proba1", []byte("1234"))
	//mem.BrziAdd("proba2", []byte("12345"))
	//mem.BrziAdd("proba1", []byte("1234"))
	//mem.BrziAdd("proba2", []byte("12345"))
	//mem.Flush(sstable)
	//fmt.Println(SSTable.Find("proba23"))
	//LSMCompaction.LSMCompaction(1)
	//kvengine.Compactions()
}
