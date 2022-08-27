package main

import (
	"Projekat/Structures/CountMinSketch"
	"Projekat/Structures/HyperLogLog"
	"Projekat/Structures/KVEngine"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func mainMenu(kvengine *KVEngine.KVEngine) {
	for {
		fmt.Println("Izaberite opciju: GET - 1, PUT - 2, DELETE - 3, exit - 0")
		fmt.Print("Opcija: ")
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		if strings.Compare(text, "1") == 0 {
			getMenu(kvengine)
		} else if strings.Compare(text, "2") == 0 {
			putMenu(kvengine)
		} else if strings.Compare(text, "3") == 0 {
			deleteMenu(kvengine)
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

	for {
		fmt.Println("Izaberite opciju: GET STRING - 1, GET HYPERLOGLOG - 2, GET COUNTMINSKETCH - 3, exit - 0")
		fmt.Print("Opcija: ")

		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		
		found, data := kvengine.Get(key)
		
		if !found {
			fmt.Println("Pod zadatim kljucem ne postoji vrednost.")
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

	for {
		fmt.Println("Izaberite opciju: PUT STRING - 1, PUT HYPERLOGLOG - 2, PUT COUNTMINSKETCH - 3, exit - 0")
		fmt.Print("Opcija: ")

		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		// u if-ovima se za string samo pozove dodavanje u bazu, a za druge dve strukture se naprave nove instance, dodaju se neki podaci u strukture, odstampaju se, serijalizuju i dodaju u bazu

		if strings.Compare(text, "1") == 0 {

			fmt.Print("Unesite vrednost: ")
			value, _ := reader.ReadString('\n')
			value = strings.Replace(value, "\n", "", -1)
			data := []byte(value)
			
			inserted := kvengine.Put(key, data)
			
			if inserted {
				fmt.Println("Uspesno je dodata vrednost pod zadatim kljucem.")
			} else {
				fmt.Println("Neuspesno dodavanje.")
			}

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

	deleted := kvengine.Delete(key)
	
	if deleted {
		fmt.Println("Uspesno je izbrisana vrednost pod zadatim kljucem.")
	} else {
		fmt.Println("Vrednost pod zadatim kljucem nije pronadjena.")
	}
}

func main() {
	//BloomFilterProba()
	//CountMinSketchProba()
	//HyperLogLogProba()
	//CacheProba()
	//TokenBucketProba()
	//settings := Settings{Path: "settings.json"}
	//settings.LoadFromJSON()
	//fmt.Println(settings)
	
	kvengine := KVEngine.MakeKVEngine()
	
	mainMenu(&kvengine)
}
