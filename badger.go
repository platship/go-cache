package cache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/rehok/go-utils/osx"
	"github.com/rehok/go-utils/timex"
)

type BadgerCache struct {
	Path string `json:"path"`

	// 值日志数据加载模式
	// 0:FileIO 从文件加载
	// 1:LoadToRAM 全部加载到内存
	// 2:MemoryMap 映射加载到内存 (默认)

	NumMemtables     int                     `json:"numMemtables"`     // 内存表数量
	MaxTableSize     int64                   `json:"maxTableSize"`     // 内存表大小（兆）
	ValueLogFileSize int64                   `json:"valueLogFileSize"` // 日志文件大小（兆）
	NumCompactors    int                     `json:"numCompactors"`    // 压缩工数量
	Compression      options.CompressionType `json:"compression"`      // 压缩方式 0:none 1:snappy 2:zstd
	SyncWrites       bool                    `json:"syncWrites"`       // 同步写 关闭可以提高性能
	GcInterval       timex.Duration          `json:"gcInterval"`       // 垃圾回收时间间隔
	GcDiscardRatio   float64                 `json:"gcDiscardRatio"`   // 垃圾回收丢弃比例

	Handle *badger.DB `json:"-"`
	onceGC sync.Once
	prefix string
}

// Set puts value into cache with key and expire time.
// If expired is 0, it lives forever.
func (b *BadgerCache) Set(key string, val interface{}, ttl int64) error {
	if err := b.undefined(); err != nil {
		return err
	}
	return b.Handle.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(b.prefix+key), val.([]byte))
		if ttl > 0 {
			e.WithTTL(time.Duration(ttl))
		}
		return txn.SetEntry(e)
	})
}

// Get gets cached value by given key.
func (b *BadgerCache) Get(key string) (res interface{}, err error) {
	if err := b.undefined(); err != nil {
		return nil, err
	}
	err = b.Handle.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(b.prefix + key))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		var vv interface{} = val
		res = vv
		return err
	})
	return
}

// Delete deletes cached value by given key.
func (b *BadgerCache) Del(key string) error {
	if err := b.undefined(); err != nil {
		return err
	}
	return b.Handle.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(b.prefix + key))
	})
}

// Incr increases cached int-type value by given key as a counter.
func (c *BadgerCache) Incr(key string) error {

	return nil
}

// Decr decreases cached int-type value by given key as a counter.
func (c *BadgerCache) Decr(key string) error {

	return nil
}

// IsExist returns true if cached value exists.
func (c *BadgerCache) Exists(key string) bool {

	return false
}

// Flush deletes all cached data.
func (c *BadgerCache) Flush() error {

	return nil
}

func (b *BadgerCache) StartAndGC(opts Options) (err error) {
	_ = b.Close()
	if b.Path == "" {
		return errors.New("path undefined")
	}

	if b.NumCompactors <= 1 {
		return errors.New("numCompactors must be > 1")
	}
	_ = osx.CreateDirIsNotExist(b.Path, 0755)

	var opt = badger.DefaultOptions(b.Path).
		WithLogger(nil).
		WithZSTDCompressionLevel(7). // zstd压缩等级
		WithNumMemtables(b.NumMemtables).
		WithNumLevelZeroTables(b.NumMemtables).
		WithNumLevelZeroTablesStall(b.NumMemtables * 2).
		WithValueLogFileSize((b.ValueLogFileSize / 2) << 20).
		WithNumCompactors(b.NumCompactors).
		WithCompression(b.Compression).
		WithSyncWrites(b.SyncWrites)

	if b.Handle, err = badger.Open(opt); err != nil {
		return err
	}
	b.onceGC.Do(func() {
		go b.autoGC()
	})

	return nil
}

func (b *BadgerCache) autoGC() {
	td := b.GcInterval.Duration()
	if td == 0 {
		b.GcInterval.Number = 5
		b.GcInterval.Unit = "minute"
		td = 5 * time.Minute
	}
	ticker := time.NewTicker(td)
	defer ticker.Stop()
	for range ticker.C {
	again:
		if err := b.RunValueLogGC(); err == nil {
			goto again
		}
	}
}
func (b *BadgerCache) RunValueLogGC() error {
	if err := b.undefined(); err != nil {
		return err
	}
	if b.GcDiscardRatio <= 0 || b.GcDiscardRatio > 1 {
		b.GcDiscardRatio = 0.9
	}
	return b.Handle.RunValueLogGC(b.GcDiscardRatio)
}

func (b *BadgerCache) undefined() error {
	if b == nil || b.Handle == nil || b.Handle.IsClosed() {
		return errors.New("client uninitialized or is closed")
	}
	return nil
}

/**
 * @desc: 存入map数据
 * @param {string} key
 * @param {interface{}} data
 * @return {*}
 */
func (c *BadgerCache) HMSet(key string, data interface{}) error {

	return nil
}

/**
 * @desc: 解析map数据
 * @param {map[string]string} val 原数据
 * @param {interface{}} dst 赋值
 * @return {*}
 */
func (c *BadgerCache) HMScan(val map[string]string, dst interface{}) (err error) {

	return nil
}

/**
 * @desc: 获取多个字段 hash数据
 * @param key 存入hash的key值
 * @param field 获取的字段
 * @return {*}
 */
func (c *BadgerCache) HMGet(key string, fields []string) (res map[string]string, err error) {

	return res, nil
}

/**
 * @desc: 获取hash数据
 * @param key 存入hash的key值
 * @param field 获取的字段
 * @return {*}
 */
func (c *BadgerCache) HGet(key, field string) (data string, err error) {

	return data, nil
}

/**
 * @desc: 存入hash数据
 * @param {string} key
 * @param {interface{}} data
 * @return {*}
 */
func (c *BadgerCache) HSet(key string, data interface{}) error {

	return nil
}

/**
 * @desc: 删除某个key的某个值
 * @param {*} key
 * @param {string} field
 * @return {*}
 */
func (c *BadgerCache) HDel(key, field string) (err error) {

	return err
}

/**
 * @desc: 获取所有hash缓存
 * @param 存入hash的key值
 * @return {*}
 */
func (c *BadgerCache) HGetAll(key string) (data map[string]string, err error) {

	return data, nil
}

/**
 * @desc: 设置有效期
 * @param {string} key
 * @param {time.Duration} expire
 * @return {*}
 */
func (c *BadgerCache) Expire(key string, expire time.Duration) error {

	return nil
}

/**
 * @desc: 清理
 * @param {*} bucket
 * @return {*}
 */
func (b *BadgerCache) Clear(key string) (err error) {
	if err := b.undefined(); err != nil {
		return err
	}
	return b.Handle.DropPrefix([]byte(b.prefix))

}

func (c *BadgerCache) Size(bucket string) string {
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

func (c *BadgerCache) TTL(key string) time.Duration {

	return 1
}

func (c *BadgerCache) Type(key string) string {

	return ""
}

func (c *BadgerCache) Search(bucket string) []string {

	return []string{}
}

func (b *BadgerCache) Close() error {
	if err := b.undefined(); err != nil {
		return err
	}
	if err := b.Handle.Close(); err != nil {
		return err
	}
	b.Handle = nil
	return nil
}

func init() {
	Register("badger", &BadgerCache{
		Path:             "/badger",
		NumMemtables:     2,
		MaxTableSize:     16,
		ValueLogFileSize: 256, // 设置512在1G内存下会占用过高
		NumCompactors:    2,
		Compression:      1,
		SyncWrites:       false,
		GcInterval:       timex.Duration{Number: 5, Unit: timex.DurationMinute},
		GcDiscardRatio:   0.9,
	})
}
