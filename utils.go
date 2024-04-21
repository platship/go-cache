package cache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fasthey/go-utils/conv"
	"github.com/fasthey/go-utils/stringx"
)

func EncodeGob(item *Item) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := gob.NewEncoder(buf).Encode(item)
	return buf.Bytes(), err
}

func DecodeGob(data []byte, out *Item) error {
	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(&out)
}

func Incr(val interface{}) (interface{}, error) {
	switch val.(type) {
	case int:
		val = val.(int) + 1
	case int32:
		val = val.(int32) + 1
	case int64:
		val = val.(int64) + 1
	case uint:
		val = val.(uint) + 1
	case uint32:
		val = val.(uint32) + 1
	case uint64:
		val = val.(uint64) + 1
	default:
		return val, errors.New("item value is not int-type")
	}
	return val, nil
}

func Decr(val interface{}) (interface{}, error) {
	switch val.(type) {
	case int:
		val = val.(int) - 1
	case int32:
		val = val.(int32) - 1
	case int64:
		val = val.(int64) - 1
	case uint:
		if val.(uint) > 0 {
			val = val.(uint) - 1
		} else {
			return val, errors.New("item value is less than 0")
		}
	case uint32:
		if val.(uint32) > 0 {
			val = val.(uint32) - 1
		} else {
			return val, errors.New("item value is less than 0")
		}
	case uint64:
		if val.(uint64) > 0 {
			val = val.(uint64) - 1
		} else {
			return val, errors.New("item value is less than 0")
		}
	default:
		return val, errors.New("item value is not int-type")
	}
	return val, nil
}

func isNotNumber(val interface{}) bool {
	switch val.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string, bool:
		return false
	default:
		return true
	}
}

func setValue(field reflect.StructField, value reflect.Value) (tag string, val interface{}, child []map[string]interface{}) {
	if field.Anonymous {
		for i := 0; i < value.NumField(); i++ {
			newTag, newVal, _ := setValue(field.Type.Field(i), value.Field(i))
			child = append(child, map[string]interface{}{"tag": newTag, "val": newVal})
		}
		return tag, val, child
	} else {
		tag := field.Tag.Get("cache")
		if tag == "" {
			tag = stringx.GetGromTag(field.Tag.Get("gorm"))
		}
		if tag != "" {
			if field.Type.Kind() == reflect.Ptr {
				val := ToStr(value)
				if val != "" {
					return tag, val, nil
				}
			} else if field.Type.Kind() == reflect.Struct {
				if value.Type().String() == "time.Time" {
					return tag, ToStr(value), nil
				}
			} else if field.Type.Kind() == reflect.Slice {
				val = conv.ByteToString(value.Bytes())
				if val != "" {
					return tag, val, nil
				}
			} else {
				return tag, value.Interface(), nil
			}
		}
	}
	return tag, nil, nil
}

/**
 * @desc: 解析字段
 * @param {map[string]string} val
 * @param {reflect.StructField} field
 * @param {reflect.Value} value
 * @return {*}
 */
func scanValue(val map[string]string, field reflect.StructField, value reflect.Value) {
	if field.Anonymous {
		for i := 0; i < value.NumField(); i++ {
			scanValue(val, field.Type.Field(i), value.Field(i))
		}
	} else {
		formTag := field.Tag.Get("redis")
		if formTag == "" {
			formTag = stringx.GetGromTag(field.Tag.Get("gorm"))
		}
		if formTag != "" {
			// 查看是否有取值
			v := val[formTag]
			if len(v) == 0 {
				return
			}
			// 根据类型来设置值
			switch fieldType := field.Type.Kind(); fieldType {
			case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
				typedV, _ := strconv.ParseInt(v, 10, 64)
				value.SetInt(typedV)
			case reflect.String:
				value.SetString(v)
			case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				typedV, _ := strconv.ParseUint(v, 10, 64)
				value.SetUint(typedV)
			case reflect.Bool:
				value.SetBool(v == "true")
			case reflect.Slice:
				value.SetBytes(conv.StringToByte(v))
			case reflect.Struct:
				switch value.Type().String() {
				case "time.Time":
					vvv := strings.Split(v, "+")
					t, err := time.Parse("2006-01-02 15:04:05 ", vvv[0])
					if err == nil {
						value.Set(reflect.ValueOf(time.Time(t)))
					}
				}
			case reflect.Ptr:
			default:
				log.Printf("field type %s not support yet", fieldType)
			}
		}
	}
}

func StringInArray(item string, items []string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

func IsExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

type argInt []int

func (a argInt) Get(i int, args ...int) (r int) {
	if i >= 0 && i < len(a) {
		r = a[i]
	} else if len(args) > 0 {
		r = args[0]
	}
	return
}

func ToStr(value interface{}, args ...int) (s string) {
	switch v := value.(type) {
	case bool:
		s = strconv.FormatBool(v)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', argInt(args).Get(0, -1), argInt(args).Get(1, 32))
	case float64:
		s = strconv.FormatFloat(v, 'f', argInt(args).Get(0, -1), argInt(args).Get(1, 64))
	case int:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int8:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int16:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int32:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int64:
		s = strconv.FormatInt(v, argInt(args).Get(0, 10))
	case uint:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint8:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint16:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint32:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint64:
		s = strconv.FormatUint(v, argInt(args).Get(0, 10))
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}
