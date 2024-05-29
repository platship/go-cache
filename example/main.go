package main

import (
	"fmt"

	"github.com/rehok/go-cache"
)

func main() {

	// redis
	// newCache, err := cache.New(cache.Options{
	// 	Adapter:       "redis",
	// 	AdapterConfig: "addr=xxxx,password=xxxx,db=db1,prefix=fasthey",
	// 	OccupyMode:    true,
	// })
	// if err == nil {
	// 	fmt.Printf("error is :%v", err)
	// }

	// file

	newCache, err := cache.New(cache.Options{
		Adapter:       "file",
		AdapterConfig: "./runtime/cache",
		OccupyMode:    true,
	})
	if err != nil {
		fmt.Printf("error is :%v", err)
	}
	err = newCache.Set("key", "is test", 0)
	if err != nil {
		fmt.Printf("set error is :%v", err)
	}
	data, err := newCache.Get("key")
	if err != nil {
		fmt.Printf("get error is :%v", err)
	}
	fmt.Println(data)
}
