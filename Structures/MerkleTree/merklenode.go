package MerkleTree

import (
	"crypto/sha1"
	"encoding/hex"
)

type MerkleNode struct {
	left      *MerkleNode
	right     *MerkleNode
	HashValue [20]byte
	IsEmpty   [1]byte
}

func (node *MerkleNode) SetLeftNode(leftNode *MerkleNode) {
	node.left = leftNode
}

func (node *MerkleNode) SetRightNode(rightNode *MerkleNode) {
	node.right = rightNode
}

func (node *MerkleNode) String() string {
	return hex.EncodeToString(node.HashValue[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

type MerkleNodeQueue struct {
	elements []*MerkleNode
}

func (queue *MerkleNodeQueue) Enqueue(element *MerkleNode) {
	queue.elements = append(queue.elements, element)
}

func (queue *MerkleNodeQueue) Dequeue() *MerkleNode {
	element := queue.elements[0]
	queue.elements = queue.elements[1:len(queue.elements)]
	return element
}

func (queue *MerkleNodeQueue) Front() *MerkleNode {
	return queue.elements[0]
}

func (queue *MerkleNodeQueue) IsEmpty() bool {
	return len(queue.elements) == 0
}

func (queue *MerkleNodeQueue) Size() int {
	return len(queue.elements)
}
