package xfredigo

import "github.com/garyburd/redigo/redis"

type Fetchable interface {
	Scan(src []interface{}) error
	Value() interface{}
	WriteCmd() string
	ReadCmd() string
}

type fetchableStruct struct {
	v interface{}
}

func Struct(v interface{}) Fetchable {
	return fetchableStruct{v: v}
}

func (f fetchableStruct) Scan(src []interface{}) error {
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
