package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func mainMenu() {
	for {
		fmt.Println("Izaberite opciju: GET - 1, PUT - 2, DELETE - 3, exit - 0")
		fmt.Print("Opcija: ")
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		if strings.Compare(text, "1") == 0 {
			getMenu()
		} else if strings.Compare(text, "2") == 0 {
			putMenu()
		} else if strings.Compare(text, "3") == 0 {
			deleteMenu()
		} else if strings.Compare(text, "0") == 0 {
			break
		} else {
			fmt.Println("Nepostojeca opcija.")
		}
	}
}

func getMenu() {
	fmt.Print("Unesite kljuc: ")
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.Replace(key, "\n", "", -1)

	for {
		fmt.Println("Izaberite opciju: GET STRING - 1, GET HYPERLOGLOG - 2, GET COUNTMINSKETCH - 3, exit - 0")
		fmt.Print("Opcija: ")

		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		// ovde da se doda izvlacenje iz baze i time se dobija niz bajtova, a onda u if-ovima se ti bajtovi konvertuju u sta treba (za string je lagano, za druga dva se poziva deserijalizacija i stampaju se strukture)

		if strings.Compare(text, "1") == 0 {

		} else if strings.Compare(text, "2") == 0 {

		} else if strings.Compare(text, "3") == 0 {

		} else if strings.Compare(text, "0") == 0 {
			break
		} else {
			fmt.Println("Nepostojeca opcija.")
		}
	}
}

func putMenu() {
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
			valueText, _ := reader.ReadString('\n')
			valueText = strings.Replace(valueText, "\n", "", -1)
			value := []byte(valueText)
			fmt.Println(value)

		} else if strings.Compare(text, "2") == 0 {

		} else if strings.Compare(text, "3") == 0 {

		} else if strings.Compare(text, "0") == 0 {
			break
		} else {
			fmt.Println("Nepostojeca opcija.")
		}
	}
}

func deleteMenu() {
	fmt.Print("Unesite kljuc: ")
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.Replace(key, "\n", "", -1)

	// ovde se poziva brisanje iz baze
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
	mainMenu()
}
