package main

import (
	"fmt"
	"time"
)

func main() {

	fmt.Println("start...", time.Now().UnixMilli())
	ticker := time.NewTicker(time.Second * 2)
	c := <-ticker.C
	fmt.Println("ticket end", c.UnixMilli())
	ticker.Stop()
	fmt.Println("ticker reset", time.Now().UnixMilli())
	ticker.Reset(time.Second * 2)
	d := <-ticker.C
	fmt.Println("ticket done ", d.UnixMilli())
	//go test(ticker)
	//
	//ticker.Reset(time.Second * 2)
	//fmt.Println("reset ticker")
	//time.Sleep(time.Second * 10)
}

func test(tick *time.Ticker) {
	fmt.Println("test... ", time.Now().UnixMilli())
	<-tick.C
	fmt.Println("test... ", time.Now().UnixMilli())
}
