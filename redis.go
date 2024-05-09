package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"gopkg.in/ini.v1"
)

// RedisCache represents a redis cache adapter implementation.
type RedisCache struct {
	client     *redis.Client
	prefix     string
	hsetName   string
	occupyMode bool
}

var ctx = context.Background()

// Set puts value into cache with key and expire time.
// If expired is 0, it lives forever.
func (c *RedisCache) Set(key string, val interface{}, expire int64) error {
	if isNotNumber(val) {
		val, _ = json.Marshal(val)
	}
	key = c.prefix + key
	if expire == 0 {
		if err := c.client.Set(ctx, key, ToStr(val), 0).Err(); err != nil {
			return err
		}
	} else {
		dur, err := time.ParseDuration(ToStr(expire) + "s")
		if err != nil {
			return err
		}
		if err = c.client.SetEx(ctx, key, ToStr(val), dur).Err(); err != nil {
			return err
		}
	}
	if c.occupyMode {
		return nil
	}
	return c.client.HSet(ctx, c.hsetName, key, "0").Err()
}

// Get gets cached value by given key.
func (c *RedisCache) Get(key string) (interface{}, error) {
	val, err := c.client.Get(ctx, c.prefix+key).Result()
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Delete deletes cached value by given key.
func (c *RedisCache) Del(key string) error {
	key = c.prefix + key
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return err
	}
	if c.occupyMode {
		return nil
	}
	return c.client.HDel(ctx, c.hsetName, key).Err()
}

// Incr increases cached int-type value by given key as a counter.
func (c *RedisCache) Incr(key string) error {
	if !c.Exists(key) {
		return fmt.Errorf("key '%s' not exist", key)
	}
	return c.client.Incr(ctx, c.prefix+key).Err()
}

// Decr decreases cached int-type value by given key as a counter.
func (c *RedisCache) Decr(key string) error {
	if !c.Exists(key) {
		return fmt.Errorf("key '%s' not exist", key)
	}
	return c.client.Decr(ctx, c.prefix+key).Err()
}

// IsExist returns true if cached value exists.
func (c *RedisCache) Exists(key string) bool {
	state, err := c.client.Exists(ctx, c.prefix+key).Result()
	if state > 0 && err == nil {
		return true
	}
	if !c.occupyMode {
		c.client.HDel(ctx, c.hsetName, c.prefix+key)
	}
	return false
}

// Flush deletes all cached data.
func (c *RedisCache) Flush() error {

	keys, err := c.client.HKeys(ctx, c.hsetName).Result()
	if err != nil {
		return err
	}
	if err = c.client.Del(ctx, keys...).Err(); err != nil {
		return err
	}
	return c.client.Del(ctx, c.hsetName).Err()
}

// StartAndGC starts GC routine based on config string settings.
// AdapterConfig: network=tcp,addr=:6379,password=123456,db=0,pool_size=100,idle_timeout=180,hset_name=Cache,prefix=cache:
func (c *RedisCache) StartAndGC(opts Options) error {

	c.hsetName = "Cache"
	c.occupyMode = opts.OccupyMode

	cfg, err := ini.Load([]byte(strings.Replace(opts.AdapterConfig, ",", "\n", -1)))
	if err != nil {
		return err
	}

	opt := &redis.Options{
		Network: "tcp",
	}
	for k, v := range cfg.Section("").KeysHash() {
		switch k {
		case "network":
			opt.Network = v
		case "addr":
			opt.Addr = v
		case "password":
			opt.Password = v
		case "db":
			opt.DB = 0
		case "pool_size":
			num, _ := strconv.Atoi(v)
			opt.PoolSize = num
		case "hset_name":
			c.hsetName = v
		case "prefix":
			c.prefix = v
		default:
			return fmt.Errorf("session/redis: unsupported option '%s'", k)
		}
	}

	c.client = redis.NewClient(opt)
	if err = c.client.Ping(ctx).Err(); err != nil {
		return err
	}

	return nil
}

/**
 * @desc: 存入map数据
 * @param {string} key
 * @param {interface{}} data
 * @return {*}
 */
func (c *RedisCache) HMSet(key string, data interface{}) error {
	if data == nil {
		return errors.New("parameter is empty")
	}
	field := reflect.TypeOf(data)
	if field.Kind() == reflect.Ptr && field.Elem().Kind() == reflect.Struct {
		return errors.New("HMSet解析失败")
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
	if err := c.client.HMSet(ctx, c.prefix+key, values).Err(); err != nil {
		return err
	}
	return nil
}

/**
 * @desc: 解析map数据
 * @param {map[string]string} val 原数据
 * @param {interface{}} dst 赋值
 * @return {*}
 */
func (c *RedisCache) HMScan(val map[string]string, dst interface{}) (err error) {
	types := reflect.TypeOf(dst)
	// 首先判断传入参数的类型
	if !(types.Kind() == reflect.Ptr && types.Elem().Kind() == reflect.Struct) {
		return errors.New("HMScan解析失败")
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
func (c *RedisCache) HMGet(key string, fields []string) (res map[string]string, err error) {
	data, err := c.client.HMGet(ctx, c.prefix+key, fields...).Result()
	if err != nil {
		return res, errors.New("Get failed")
	}
	newData := make(map[string]string)
	for i, v := range data {
		if v != nil {
			newData[fields[i]] = ToStr(v)
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
func (c *RedisCache) HGet(key, field string) (data string, err error) {
	if c.client == nil {
		return data, errors.New("Redis Error")
	}
	if key == "" {
		return data, errors.New("parameter is empty")
	}
	data, err = c.client.HGet(ctx, c.prefix+key, field).Result()
	if err != nil {
		return data, errors.New("Get failed")
	}
	return data, nil
}

/**
 * @desc: 存入hash数据
 * @param {string} key
 * @param {interface{}} data
 * @return {*}
 */
func (c *RedisCache) HSet(key string, data interface{}) (bool, error) {
	err := c.client.HSet(ctx, c.prefix+key, data).Err()
	if err != nil {
		return false, errors.New("add failed")
	}
	return true, nil
}

/**
 * @desc: 删除某个key的某个值
 * @param {*} key
 * @param {string} field
 * @return {*}
 */
func (c *RedisCache) HDel(key, field string) (err error) {
	err = c.client.HDel(ctx, c.prefix+key, field).Err()
	return err
}

/**
 * @desc: 获取所有hash缓存
 * @param 存入hash的key值
 * @return {*}
 */
func (c *RedisCache) HGetAll(key string) (data map[string]string, err error) {
	if key == "" {
		return data, errors.New("parameter is empty")
	}
	data, err = c.client.HGetAll(ctx, c.prefix+key).Result()
	if err != nil {
		return data, err
	}
	return data, nil
}

/**
 * @desc: 设置有效期
 * @param {string} key
 * @param {time.Duration} expire
 * @return {*}
 */
func (c *RedisCache) Expire(key string, expire time.Duration) bool {
	state, err := c.client.Expire(ctx, c.prefix+key, expire).Result()
	if state || err == nil {
		return true
	}
	return false
}

/**
 * @desc: 清理
 * @param {*} bucket
 * @return {*}
 */
func (c *RedisCache) Clear(bucket string) (err error) {
	iter := c.client.Scan(ctx, 0, c.prefix+bucket+"*", 0).Iterator()
	for iter.Next(ctx) {
		if err := c.client.Del(ctx, iter.Val()).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (c *RedisCache) Size(bucket string) string {
	info, err := c.client.Info(ctx, "memory").Result()
	if err != nil {
		return "0"
	}
	usedMemory := parseMemoryInfo(info)
	return usedMemory
}

// 解析INFO命令返回的内存信息
func parseMemoryInfo(info string) string {
	for _, line := range splitInfo(info) {
		if isMemoryLine(line) {
			return line[len("used_memory:"):]
		}
	}
	return "0"
}

// 分割INFO命令返回的字符串
func splitInfo(response string) map[string]string {
	info := make(map[string]string)
	lines := strings.Split(response, "\r\n")
	for _, line := range lines {
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			info[parts[0]] = parts[1]
		}
	}
	return info
}

// 检查是否是描述内存使用的行
func isMemoryLine(line string) bool {
	return len(line) > 12 && line[:12] == "used_memory:"
}

func init() {
	Register("redis", &RedisCache{})
}
