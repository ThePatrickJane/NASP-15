package SkipList

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

type SkipList struct {
	maxHeight   int
	height      int
	currentSize int
	head        *Node
}

type Node struct {
	key       string
	value     []byte
	timestamp int64
	tombstone bool
	next      []*Node
}

type Content struct {
	Key       string
	Value     []byte
	Timestamp int64
	Tombstone bool
}

func (node *Node) GetKey() string {
	return node.key
}

func (node *Node) GetValue() []byte {
	return node.value
}

func (node *Node) GetTimestamp() int64 {
	return node.timestamp
}

func (node *Node) GetTombstone() bool {
	return node.tombstone
}

func (list *SkipList) GetCurrentHeight() int {
	return list.height
}

func (list *SkipList) GetMaxHeight() int {
	return list.maxHeight
}

func (list *SkipList) GetSize() int {
	return list.currentSize
}

func New(maxHeight int) *SkipList {
	return &SkipList{
		maxHeight:   maxHeight,
		height:      0,
		currentSize: 0,
		head: &Node{
			key:   "",
			value: nil,
			next:  make([]*Node, maxHeight),
		},
	}
}

func (list *SkipList) randomLevel() int {
	level := 0

	for ; rand.Int31n(2) == 1; level++ {

		if level == list.maxHeight {
			return level
		}
		if level > list.height {

			list.height = level
			return level
		}
	}
	return level
}

func (list *SkipList) Add(key string, value []byte, tombstone bool) Content {

	if list.Contains(key) {
		updatedContent, err := list.Update(key, value)
		if err == nil {
			return updatedContent
		}

	}

	level := list.randomLevel()
	newNode := &Node{
		key:       key,
		value:     value,
		timestamp: time.Now().Unix(),
		tombstone: tombstone,
		next:      make([]*Node, level+1),
	}

	starterNode := list.head

	// loop za svaki nivo
	for currentHeight := list.height; currentHeight >= 0; currentHeight-- {

		currentNode := starterNode
		// loop kroz cvaki cvor na odredjenom nivou
		for ; currentNode.next[currentHeight] != nil; currentNode = currentNode.next[currentHeight] {

			if currentNode.next[currentHeight].key < key {
				starterNode = currentNode.next[currentHeight]

			} else if currentNode.next[currentHeight].key > key {
				break
			}
		}

		// insert cvora samo ukoliko smemo na tom nivou
		if currentHeight <= level {
			newNode.next[currentHeight] = starterNode.next[currentHeight]
			starterNode.next[currentHeight] = newNode
		}
	}
	list.currentSize++
	return Content{
		Key:       newNode.key,
		Value:     newNode.value,
		Timestamp: newNode.timestamp,
		Tombstone: newNode.tombstone,
	}
}

func (list *SkipList) get(key string) *Node {

	starterNode := list.head
	// loop za svaki nivo
	for currentHeight := list.height; currentHeight >= 0; currentHeight-- {

		// loop kroz cvaki cvor na odredjenom nivou
		for currentNode := starterNode; currentNode.next[currentHeight] != nil; currentNode = currentNode.next[currentHeight] {

			if currentNode.next[currentHeight].key > key {
				break

			} else if currentNode.next[currentHeight].key < key {
				starterNode = currentNode.next[currentHeight]

			} else if currentNode.next[currentHeight].key == key {
				return currentNode.next[currentHeight]
			}
		}

	}
	return nil
}

func (list *SkipList) Get(key string) (Content, error) {
	node := list.get(key)
	if node != nil {
		return Content{
			Key:       node.key,
			Value:     node.value,
			Timestamp: node.timestamp,
			Tombstone: node.tombstone,
		}, nil
	}

	return Content{}, errors.New("key not found")
}

func (list *SkipList) Update(key string, value []byte) (Content, error) {
	node := list.get(key)
	if node != nil {
		node.value = value
		node.timestamp = time.Now().Unix()
		fmt.Println("Element is updated!")
		return Content{
			Key:       node.key,
			Value:     node.value,
			Tombstone: node.tombstone,
			Timestamp: node.timestamp,
		}, nil
	}
	return Content{}, errors.New("key not found")
}

func (list *SkipList) LogDelete(key string) bool {
	node := list.get(key)
	if node != nil {
		node.timestamp = time.Now().Unix()
		node.tombstone = true
		return true
	}
	return false
}

func (list *SkipList) Delete(key string) (Content, error) {

	isDeleted := false
	starterNode := list.head
	delContent := Content{}

	// loop za sve nivoe
	for currentHeight := list.height; currentHeight >= 0; currentHeight-- {

		for currentNode := starterNode; currentNode.next[currentHeight] != nil; currentNode = currentNode.next[currentHeight] {
			if currentNode.next[currentHeight].key > key {
				break

			} else if currentNode.next[currentHeight].key < key {
				starterNode = currentNode.next[currentHeight]

			} else if currentNode.next[currentHeight].key == key {

				delContent.Key = currentNode.next[currentHeight].key
				delContent.Value = currentNode.next[currentHeight].value
				delContent.Tombstone = currentNode.next[currentHeight].tombstone
				delContent.Timestamp = currentNode.next[currentHeight].timestamp

				isDeleted = true
				starterNode.next[currentHeight] = currentNode.next[currentHeight].next[currentHeight]

				// uredjenje visine liste ako nema vise cvorova na tom nivou
				if starterNode.next[currentHeight] == nil {
					list.height--
				}
				break
			}
		}
	}
	if isDeleted {
		list.currentSize--
		return delContent, nil
	} else {
		return Content{}, errors.New("key not found")
	}
}

func (list *SkipList) Contains(key string) bool {

	_, err := list.Get(key)
	if err != nil {
		return false
	}
	return true
}

func (list *SkipList) DisplayLevel(level int) {

	fmt.Println("level", level)
	for currentNode := list.head; currentNode.next[level] != nil; currentNode = currentNode.next[level] {
		fmt.Println(currentNode.next[level].key, currentNode.next[level].value)
	}
}

func (list *SkipList) GetElements() []Content {
	contents := make([]Content, 0)
	for currentNode := list.head; currentNode.next[0] != nil; currentNode = currentNode.next[0] {
		content := Content{
			Key:       currentNode.next[0].key,
			Value:     currentNode.next[0].value,
			Timestamp: currentNode.next[0].timestamp,
			Tombstone: currentNode.next[0].tombstone,
		}
		contents = append(contents, content)
	}
	return contents
}

func (list *SkipList) ToMap() map[string]Content {
	mapOfContent := map[string]Content{}
	list.toMap(list.head.next, mapOfContent)
	return mapOfContent
}

func (list *SkipList) toMap(nodes []*Node, mapOfContent map[string]Content) {

	for _, node := range nodes {

		if node == nil {
			continue

		} else {
			if _, isKeyAdded := mapOfContent[node.key]; !isKeyAdded {
				mapOfContent[node.key] = Content{
					Key:       node.key,
					Value:     node.value,
					Timestamp: node.timestamp,
					Tombstone: node.tombstone,
				}
				list.toMap(node.next, mapOfContent)
			} else {
				continue
			}
		}
	}
}

func (list *SkipList) Clear() {
	list.head = &Node{
		key:   "",
		value: nil,
		next:  make([]*Node, list.maxHeight),
	}
	list.height = 0
	list.currentSize = 0
}

func (list *SkipList) ReconstructionInsert(key string, value []byte, tombstone bool, timestamp int64) Content{
	if list.Contains(key) {
		updatedContent, err := list.Update(key, value)
		if err == nil {
			return updatedContent
		}

	}

	level := list.randomLevel()
	newNode := &Node{
		key:       key,
		value:     value,
		timestamp: timestamp,
		tombstone: tombstone,
		next:      make([]*Node, level+1),
	}

	starterNode := list.head

	// loop za svaki nivo
	for currentHeight := list.height; currentHeight >= 0; currentHeight-- {

		currentNode := starterNode
		// loop kroz cvaki cvor na odredjenom nivou
		for ; currentNode.next[currentHeight] != nil; currentNode = currentNode.next[currentHeight] {

			if currentNode.next[currentHeight].key < key {
				starterNode = currentNode.next[currentHeight]

			} else if currentNode.next[currentHeight].key > key {
				break
			}
		}

		// insert cvora samo ukoliko smemo na tom nivou
		if currentHeight <= level {
			newNode.next[currentHeight] = starterNode.next[currentHeight]
			starterNode.next[currentHeight] = newNode
		}
	}
	list.currentSize++
	return Content{
		Key:       newNode.key,
		Value:     newNode.value,
		Timestamp: newNode.timestamp,
		Tombstone: newNode.tombstone,
	}
}
