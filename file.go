package cache

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Item represents a cache item.
type Item struct {
	Val     interface{}
	Created int64
	Expire  int64
}

func (item *Item) hasExpired() bool {
	return item.Expire > 0 &&
		(time.Now().Unix()-item.Created) >= item.Expire
}

// FileCache represents a file cache adapter implementation.
type FileCache struct {
	lock     sync.Mutex
	rootPath string
	interval int // GC interval.
}

// NewFileCache creates and returns a new file cacher.
func NewFileCache() *FileCache {
	return &FileCache{}
}

func (c *FileCache) filepath(key string) string {
	keys := strings.Split(key, "_")
	var path string
	if len(keys) >= 2 {
		path = "/" + keys[0]
	}
	m := md5.Sum([]byte(key))
	hash := hex.EncodeToString(m[:])
	return filepath.Join(c.rootPath+path, string(hash[0]), string(hash[1]), hash)
}

// Set puts value into cache with key and expire time.
// If expired is 0, it will be deleted by next GC operation.
func (c *FileCache) Set(key string, val interface{}, expire int64) error {
	filename := c.filepath(key)
	if isNotNumber(val) {
		val, _ = json.Marshal(val)
	}
	item := &Item{val, time.Now().Unix(), expire}
	data, err := EncodeGob(item)
	if err != nil {
		return err
	}
	os.MkdirAll(filepath.Dir(filename), os.ModePerm)
	return os.WriteFile(filename, data, os.ModePerm)
}

func (c *FileCache) read(key string) (*Item, error) {
	filename := c.filepath(key)

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	item := new(Item)
	return item, DecodeGob(data, item)
}

// Get gets cached value by given key.
func (c *FileCache) Get(key string) (interface{}, error) {
	item, err := c.read(key)
	if err != nil {
		return nil, err
	}
	if item.hasExpired() {
		os.Remove(c.filepath(key))
		return nil, err
	}
	if isNotNumber(item.Val) {
		val := item.Val.([]byte)
		json.Unmarshal(val, &item.Val)
	} else {
		item.Val = fmt.Sprintf("%v", item.Val)
	}
	return item.Val, nil
}

// Delete deletes cached value by given key.
func (c *FileCache) Del(key string) error {
	return os.Remove(c.filepath(key))
}

// Incr increases cached int-type value by given key as a counter.
func (c *FileCache) Incr(key string) error {
	item, err := c.read(key)
	if err != nil {
		return err
	}

	item.Val, err = Incr(item.Val)
	if err != nil {
		return err
	}

	return c.Set(key, item.Val, item.Expire)
}

// Decrease cached int value.
func (c *FileCache) Decr(key string) error {
	item, err := c.read(key)
	if err != nil {
		return err
	}

	item.Val, err = Decr(item.Val)
	if err != nil {
		return err
	}

	return c.Set(key, item.Val, item.Expire)
}

// Exists returns true if cached value exists.
func (c *FileCache) Exists(key string) bool {
	return IsExist(c.filepath(key))
}

// Flush deletes all cached data.
func (c *FileCache) Flush() error {
	return os.RemoveAll(c.rootPath)
}

func (c *FileCache) startGC() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.interval < 1 {
		return
	}

	if err := filepath.Walk(c.rootPath, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("Walk: %v", err)
		}

		if fi.IsDir() {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			fmt.Errorf("readFile: %v", err)
		}

		item := new(Item)
		if err = DecodeGob(data, item); err != nil {
			return err
		}
		if item.hasExpired() {
			if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove: %v", err)
			}
		}
		return nil
	}); err != nil {
		log.Printf("error garbage collecting cache files: %v", err)
	}

	time.AfterFunc(time.Duration(c.interval)*time.Second, func() { c.startGC() })
}

// StartAndGC starts GC routine based on config string settings.
func (c *FileCache) StartAndGC(opt Options) error {
	c.lock.Lock()
	exePath, _ := os.Getwd()
	c.rootPath = exePath + "/" + strings.Replace(opt.AdapterConfig, "./", "", 0)
	c.interval = opt.Interval

	if !filepath.IsAbs(c.rootPath) {
		c.rootPath = filepath.Join("/", c.rootPath)
	}
	c.lock.Unlock()

	if err := os.MkdirAll(c.rootPath, os.ModePerm); err != nil {
		return err
	}

	go c.startGC()
	return nil
}

/**
 * @desc: 存入map数据
 * @param {string} key
 * @param {interface{}} data
 * @return {*}
 */
func (c *FileCache) HMSet(key string, data interface{}) error {
	if key == "" || data == nil {
		return errors.New("parameter is empty")
	}
	field := reflect.TypeOf(data)
	if field.Kind() == reflect.Ptr && field.Elem().Kind() == reflect.Struct {
		return errors.New("parsing failed")
	}
	value := reflect.ValueOf(data)
	values := make(map[string]interface{})
	for i := 0; i < value.NumField(); i++ {
		tag, val, child := setValue(field.Field(i), value.Field(i))
		if child != nil {
			for _, v := range child {
				if v["tag"].(string) != "" || v["val"] != nil {
					values[v["tag"].(string)] = v["val"]
				}
			}
		} else {
			if val != "" && val != nil {
				values[tag] = val
			}
		}
	}
	err := c.Set(key, values, 0)
	if err != nil {
		return errors.New("add failed")
	}
	return nil
}

/**
 * @desc: 解析map数据
 * @param {map[string]string} val 原数据
 * @param {interface{}} dst 赋值
 * @return {*}
 */
func (c *FileCache) HMScan(val map[string]string, dst interface{}) (err error) {
	types := reflect.TypeOf(dst)
	// 首先判断传入参数的类型
	if !(types.Kind() == reflect.Ptr && types.Elem().Kind() == reflect.Struct) {
		return errors.New("parsing failed")
	}
	// 拿到指针所指向的元素的类型
	types = types.Elem()
	// 拿到指针所指向的元素的值
	value := reflect.ValueOf(dst).Elem()
	// 遍历每一个字段
	for i := 0; i < value.NumField(); i++ {
		scanValue(val, types.Field(i), value.Field(i))
	}
	return nil
}

/**
 * @desc: 获取多个字段 hash数据
 * @param key 存入hash的key值
 * @param field 获取的字段
 * @return {*}
 */
func (c *FileCache) HMGet(key string, fields []string) (res map[string]string, err error) {
	if !c.Exists(key) {
		return res, errors.New("does not exist")
	}
	data, err := c.HGetAll(key)
	if err != nil {
		return res, err
	}
	newData := make(map[string]string)
	if err == nil {
		for k, v := range data {
			if StringInArray(k, fields) {
				newData[k] = v
			}
		}
	}
	return newData, nil
}

/**
 * @desc: 获取hash数据
 * @param key 存入hash的key值
 * @param field 获取的字段
 * @return {*}
 */
func (c *FileCache) HGet(key, field string) (res string, err error) {
	if !c.Exists(key) {
		return res, errors.New("does not exist")
	}
	data, err := c.HGetAll(key)
	if err == nil {
		for k, v := range data {
			if k == field {
				return v, nil
			}
		}
	}
	return res, errors.New("does not exist")
}

/**
 * @desc: 存入hash数据
 * @param {string} key
 * @param {interface{}} data
 * @return {*}
 */
func (c *FileCache) HSet(key string, newData interface{}) (bool, error) {
	if !c.Exists(key) {
		return false, nil
	}
	data, err := c.HGetAll(key)
	if err == nil {
		if newData != nil {
			if reflect.TypeOf(newData).Kind() == reflect.Map {
				for k, v := range newData.(map[string]interface{}) {
					data[k] = ToStr(v)
				}
			} else {
				return false, errors.New("data must be map")
			}
		}
		c.Set(key, data, 0)
	}
	return true, nil
}

/**
 * @desc: 删除某个key的某个值
 * @param {*} key
 * @param {string} field
 * @return {*}
 */
func (c *FileCache) HDel(key, field string) (err error) {
	if !c.Exists(key) {
		return nil
	}
	data, err := c.HGetAll(key)
	if err == nil {
		delete(data, field)
		return c.Set(key, data, 0)
	}
	return nil
}

/**
 * @desc: 获取所有hash缓存
 * @param 存入hash的key值
 * @return {*}
 */
func (c *FileCache) HGetAll(key string) (data map[string]string, err error) {
	if !c.Exists(key) {
		return data, errors.New("is empty")
	}
	newData, err := c.Get(key)
	if err != nil {
		return data, errors.New("Get failed")
	}
	if newData != nil {
		data = make(map[string]string)
		newVal := newData.(map[string]interface{})
		for i, v := range newVal {
			data[i] = ToStr(v)
		}
	}
	return data, nil
}

/**
 * @desc: 设置有效期
 * @param {string} key
 * @param {time.Duration} expire
 * @return {*}
 */
func (c *FileCache) Expire(key string, expire time.Duration) bool {
	if !c.Exists(key) {
		return false
	}
	data, err := c.Get(key)
	if err == nil {
		if err := c.Set(key, data, int64(expire)); err == nil {
			return true
		}
	}
	return false
}

/**
 * @desc: 清理
 * @param {*} bucket
 * @return {*}
 */
func (c *FileCache) Clear(key string) (err error) {
	if !strings.Contains(key, "_") {
		return os.RemoveAll(c.rootPath + "/" + key)
	}
	return os.Remove(c.filepath(key))
}

func (c *FileCache) Size(bucket string) string {
	var size uint64
	filepath.Walk(bucket, func(_ string, info os.FileInfo, err error) error {
		if info == nil {
			return nil
		}
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return nil
	})
	return fmt.Sprintf("%d", size)
}

func init() {
	Register("file", NewFileCache())
}
