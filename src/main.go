package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {

	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)

	go loop(cond, mu)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		cond.Signal()
	}
	time.Sleep(time.Second)
}

func loop(cond *sync.Cond, mu *sync.Mutex) bool {

	i := 0
	for {
		i++
		mu.Lock()
		cond.Wait()
		mu.Unlock()
		fmt.Println(i)

	}
}
