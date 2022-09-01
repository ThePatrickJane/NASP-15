package MerkleTree

import (
	"io/ioutil"
	"os"
)

type MerkleTree struct {
	root *MerkleNode
	Size int
}

func (mt *MerkleTree) Form(data []string) {
	if len(data) == 0 {
		var emptyHash [20]byte
		mt.root = &MerkleNode{nil, nil, emptyHash, [1]byte{1}}
		mt.Size++
		return
	}
	leafNodes := mt.getLeafNodes(data)
	mt.formOneLevel(leafNodes)
}

func (mt *MerkleTree) BreadthSearch(nodeFunc func(node *MerkleNode)) {
	waitList := MerkleNodeQueue{}
	node := mt.root
	waitList.Enqueue(node)
	for !waitList.IsEmpty() {
		node = waitList.Dequeue()
		if node != nil {
			nodeFunc(node)
			waitList.Enqueue(node.left)
			waitList.Enqueue(node.right)
		}
	}
}

func (mt *MerkleTree) DepthSearch(nodeFunc func(node *MerkleNode)) {
	mt.preOrderDepthSearch(mt.root, nodeFunc)
}

func (mt *MerkleTree) getLeafNodes(data []string) []*MerkleNode {
	leafNodes := make([]*MerkleNode, len(data))
	for i, d := range data {
		node := &MerkleNode{nil, nil, Hash([]byte(d)), [1]byte{0}}
		leafNodes[i] = node
	}
	return leafNodes
}

func (mt *MerkleTree) formOneLevel(nodes []*MerkleNode) {
	if len(nodes) == 1 {
		mt.root = &MerkleNode{nil, nil, nodes[0].HashValue, [1]byte{0}}
		return
	}

	mt.Size += len(nodes)
	iterLength := len(nodes)
	parentLength := iterLength / 2
	addEmptyNode := false
	if iterLength%2 == 1 {
		iterLength--
		addEmptyNode = true
		parentLength++
	}

	parentNodes := make([]*MerkleNode, parentLength)
	for i := 0; i < iterLength; i += 2 {
		child1 := nodes[i]
		child2 := nodes[i+1]
		parentHashValue := Hash(append(child1.HashValue[:], child2.HashValue[:]...))
		parent := &MerkleNode{child1, child2, parentHashValue, [1]byte{0}}
		parentNodes[i/2] = parent
	}

	if addEmptyNode {
		emptyNode := &MerkleNode{nil, nil, Hash(nil), [1]byte{1}}
		parentHashValue := Hash(append(nodes[iterLength].HashValue[:], emptyNode.HashValue[:]...))
		parent := &MerkleNode{nodes[iterLength], emptyNode, parentHashValue, [1]byte{0}}
		parentNodes[parentLength-1] = parent
		mt.Size++
	}

	if len(parentNodes) > 1 {
		mt.formOneLevel(parentNodes)
	} else {
		mt.root = parentNodes[0]
		mt.Size++
	}
}

func (mt *MerkleTree) preOrderDepthSearch(node *MerkleNode, nodeFunc func(node *MerkleNode)) {
	if node != nil {
		nodeFunc(node)
		mt.preOrderDepthSearch(node.left, nodeFunc)
		mt.preOrderDepthSearch(node.right, nodeFunc)
	}
}

func (mt *MerkleTree) Serialize(filePath string) {
	serIndex := 0
	hashValuesForSerialization := make([]byte, 21*mt.Size)
	mt.BreadthSearch(func(node *MerkleNode) {
		for i := 0; i < 20; i++ {
			hashValuesForSerialization[21*serIndex+i] = node.HashValue[i]
		}
		hashValuesForSerialization[21*serIndex+20] = node.IsEmpty[0]
		serIndex++
	})
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0222)
	if err != nil {
		file, err = os.Create(filePath)
		if err != nil {
			panic(err)
		}
	}

	_, _ = file.Write(hashValuesForSerialization)
	_ = file.Sync()
	_ = file.Close()
}

func (mt *MerkleTree) Deserialize(filePath string) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0444)
	if err != nil {
		panic(err)
	}

	hashValues, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	nodes := make([]*MerkleNode, 0)
	for i := 0; i < len(hashValues); i += 21 {
		var hashValue [20]byte
		for j := 0; j < 20; j++ {
			hashValue[j] = hashValues[i : i+21][j]
		}
		nodes = append(nodes, &MerkleNode{nil, nil, hashValue, [1]byte{hashValues[i : i+21][20]}})
	}
	childOffset := 1
	for i := 0; i < len(nodes); i++ {
		if i+childOffset >= len(nodes) {
			break
		}
		if nodes[i].IsEmpty[0] != 0 {
			childOffset--
			continue
		}
		nodes[i].SetLeftNode(nodes[i+childOffset])
		nodes[i].SetRightNode(nodes[i+1+childOffset])
		childOffset++
	}
	mt.root = nodes[0]

	_ = file.Close()
}
