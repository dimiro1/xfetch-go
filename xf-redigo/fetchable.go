package xfredigo

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v4"

	xf "github.com/Onefootball/xfetch-go"
	"github.com/garyburd/redigo/redis"
)

type fetchableStruct struct {
	v interface{}
}

type fetchableJSONString struct{ v interface{} }

// Struct wraps the struct passed into it as a Fetchable. When a value is fetched from the cache or recomputed
// it will be copied into v.
//
// v MUST be a pointer to a struct.
func Struct(v interface{}) xf.Fetchable {
	return fetchableStruct{v: v}
}

func (f fetchableStruct) Serialize() ([]interface{}, error) {
	return redis.Args{}.AddFlat(f.v), nil
}

func (f fetchableStruct) Deserialize(reply interface{}) error {
	src, err := redis.Values(reply, nil)
	if err != nil {
		return err
	}

	return redis.ScanStruct(src, f.v)
}

func (f fetchableStruct) Unwrap() interface{} {
	return f.v
}

func (f fetchableStruct) WriteCmd() string {
	return "HMSET"
}

func (f fetchableStruct) ReadCmd() string {
	return "HGETALL"
}

func JSON(v interface{}) xf.Fetchable {
	return fetchableJSONString{v}
}

func (f fetchableJSONString) Serialize() ([]interface{}, error) {
	b, err := json.Marshal(f.v)
	if err != nil {
		return nil, err
	}

	return []interface{}{b}, nil
}

func (f fetchableJSONString) Deserialize(reply interface{}) error {
	src, err := redis.Bytes(reply, nil)
	if err != nil {
		return err
	}

	return json.Unmarshal(src, f.v)
}

func (f fetchableJSONString) Unwrap() interface{} {
	return f.v
}

func (f fetchableJSONString) WriteCmd() string {
	return "SET"
}

func (f fetchableJSONString) ReadCmd() string {
	return "GET"
}

type fetchableMsgpack struct {
	v interface{}
}

func Msgpack(v interface{}) xf.Fetchable {
	return fetchableMsgpack{v: v}
}

func (f fetchableMsgpack) Serialize() ([]interface{}, error) {
	b, err := msgpack.Marshal(f.v)
	if err != nil {
		return nil, err
	}
	return []interface{}{b}, nil
}

func (f fetchableMsgpack) Deserialize(reply interface{}) error {
	src, err := redis.Bytes(reply, nil)
	if err != nil {
		return err
	}

	return msgpack.Unmarshal(src, f.v)
}

func (f fetchableMsgpack) Unwrap() interface{} {
	return f.v
}

func (fetchableMsgpack) WriteCmd() string {
	return "SET"
}

func (fetchableMsgpack) ReadCmd() string {
	return "GET"
}
