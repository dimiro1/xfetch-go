package xfredigo

import (
	"encoding/json"

	"github.com/garyburd/redigo/redis"
)

// Fetchable is an object that can be fetched from the cache.
type Fetchable interface {
	Scan(reply interface{}) error // Scan is a function called when scanning from Redis
	Value() interface{}           // Value returns the underlying value
	WriteCmd() string             // WriteCmd is the command the library will use when writing to Redis
	ReadCmd() string              // ReadCmd is the command the library will use when writing to Redis
}

type fetchableStruct struct {
	v interface{}
}

type fetchableJSONString struct{ v interface{} }

// Struct wraps the struct passed into it as a Fetchable. When a value is fetched from the cache or recomputed
// it will be copied into v.
//
// v MUST be a pointer to a struct.
func Struct(v interface{}) Fetchable {
	return fetchableStruct{v: v}
}

func (f fetchableStruct) Scan(reply interface{}) error {
	src, err := redis.Values(reply, nil)
	if err != nil {
		return err
	}

	return redis.ScanStruct(src, f.v)
}

func (f fetchableStruct) Value() interface{} {
	return f.v
}

func (f fetchableStruct) WriteCmd() string {
	return "HMSET"
}

func (f fetchableStruct) ReadCmd() string {
	return "HGETALL"
}

func JSON(v interface{}) Fetchable {
	return fetchableJSONString{v}
}

func (f fetchableJSONString) Scan(reply interface{}) error {
	src, err := redis.Bytes(reply, nil)
	if err != nil {
		return err
	}

	return json.Unmarshal(src, f.v)
}

func (f fetchableJSONString) Value() interface{} {
	return f.v
}

func (f fetchableJSONString) WriteCmd() string {
	return "SET"
}

func (f fetchableJSONString) ReadCmd() string {
	return "GET"
}
