package Cache

import (
	"container/list"
	"fmt"
)

type Record struct {
	key string
	value []byte
}

type Cache struct {
	capacity uint64
	lookup map[string]*list.Element
	data list.List
}

func MakeCache(capacity uint64) Cache {
	return Cache{
		capacity: capacity,
		lookup:   make(map[string]*list.Element, capacity),
		data:     list.List{},
	}
}

func (c *Cache) Get(key string) (bool, []byte) {
	elem, found := c.lookup[key]

	if !found {
		return false, nil
	}

	c.data.MoveToFront(elem)

	data := elem.Value.(list.Element).Value.(Record).value

	return true, data
}

func (c *Cache) Put(key string, value []byte) {
	elem, found := c.lookup[key]

	if found {
		elem.Value = Record{key: key, value: value}
		c.data.MoveToFront(elem)
		return
	}

	if c.data.Len() == int(c.capacity) {
		backElement := c.data.Back()
		c.data.Remove(backElement)
		delete(c.lookup, backElement.Value.(list.Element).Value.(Record).key)
	}

	newElement := c.data.PushFront(list.Element{Value: Record{
		key:   key,
		value: value,
	}})

	c.lookup[key] = newElement
}

func (c *Cache) Remove(key string) {
	el := c.lookup[key]

	if el == nil {
		return
	}

	c.data.Remove(el)
	delete(c.lookup, el.Value.(list.Element).Value.(Record).key)
}

func CacheProba() { // 2 3 4 1   3 4 1 5
	c := MakeCache(4)
	c.Put("key1", []byte("milos"))
	c.Put("key2", []byte("mladen"))
	c.Put("key3", []byte("pera"))
	c.Put("key4", []byte("zika"))
	c.Remove("key6")
	fmt.Println(c)
	//fmt.Println(c.Get("key1"))
	//c.Put("key5", []byte("stoja"))
	//fmt.Println(c.Get("key2"))
	//fmt.Println(c.Get("key3"))
	//c.Remove("key1")
	//fmt.Println(c.Get("key5"))
	//fmt.Println(c)
	//for e := c.data.Front(); e != nil; e = e.Next() {
	//	fmt.Println(e)
	//}
}
