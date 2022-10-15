package main

import (
	"6.824/mr"
	"bytes"
	"encoding/gob"
	"fmt"
)

//
//func main() {
//	skipList := NewSkipList()
//	for i := 0; i < 10000; i++ {
//		skipList.Insert("key-"+strconv.Itoa(rand.Int()), "1")
//	}
//
//	curr := skipList.Header.Nexts[0]
//	res := make([]string, 10000)
//	for curr != nil {
//		res = append(res, curr.Key)
//		curr = curr.Nexts[0]
//	}
//	reverseNum := 0
//	for i := 0; i < len(res)-1; i++ {
//		if strings.Compare(res[i], res[i+1]) > 0 {
//			reverseNum++
//		}
//	}
//	fmt.Println(reverseNum)
//
//}
//
//const MaxHeightSkipList = 7
//
//type Node struct {
//	Key   string
//	Value interface{}
//	Nexts []*Node
//
//	Height int
//}
//
//type SkipList struct {
//	MaxHeight int
//	Header    *Node
//}
//
//func NewSkipList() *SkipList {
//	return &SkipList{
//		MaxHeight: 1,
//		Header: &Node{
//			Key:    "",
//			Value:  nil,
//			Nexts:  make([]*Node, MaxHeightSkipList),
//			Height: MaxHeightSkipList,
//		},
//	}
//}
//
////构建prevs
////随机level
////设置next
//func (s *SkipList) Insert(key string, value string) {
//
//	prevs := make([]*Node, MaxHeightSkipList)
//	n := s.FindGreaderOrEqual(key, prevs)
//	if n != nil && strings.Compare(n.Key, key) == 0 {
//		n.Value = value
//		return
//	}
//	level := Randowm()
//	newNode := &Node{
//		Key:    key,
//		Value:  value,
//		Nexts:  make([]*Node, level),
//		Height: level,
//	}
//	if level > s.MaxHeight {
//		//
//		for i := s.MaxHeight; i < level; i++ {
//			prevs[i] = s.Header
//		}
//		s.MaxHeight = level
//	}
//	for i := 0; i < level; i++ {
//		newNode.Nexts[i] = prevs[i].Nexts[i]
//		prevs[i].Nexts[i] = newNode
//	}
//}
//
//func Randowm() int {
//
//	level := 1
//	for level < MaxHeightSkipList && rand.Int()&1 == 0 {
//		level++
//	}
//	return level
//}
//func (s *SkipList) FindGreaderOrEqual(key string, prevs []*Node) *Node {
//	level := s.MaxHeight - 1
//	x := s.Header
//
//	for true {
//		n := x.Nexts[level]
//		if n != nil && strings.Compare(key, n.Key) > 0 {
//			//找到第一个比key大的元素
//			x = n
//		} else {
//			if prevs != nil {
//				prevs[level] = x //保存目标节点每一层前一个元素指针
//			}
//			if level == 0 {
//				return n
//			}
//			level--
//		}
//	}
//	return nil
//}

func main() {

	m1 := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}
	m2 := map[string]string{}

	b := new(bytes.Buffer)
	encoder := gob.NewEncoder(b)
	encoder.Encode(m1)

	r := bytes.NewBuffer(b.Bytes())
	decoder := gob.NewDecoder(r)

	decoder.Decode(&m2)

	fmt.Println(mr.Any2String(m2))

}
