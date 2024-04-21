# go-cache
Generic cache use and cache manager for golang.
Provide a unified usage API by packaging various commonly used drivers.

# Install

> go get github.com/fasthey/go-cache

# Cache Interface

# Usage

```
package main

import (
	"fmt"

	"github.com/fasthey/go-cache"
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
		AdapterConfig: "cache",
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

```