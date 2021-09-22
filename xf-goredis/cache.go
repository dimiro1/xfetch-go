package xfgoredis

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	xf "github.com/Onefootball/xfetch-go"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

type cache struct {
	client redis.UniversalClient
}

func Wrap(client redis.UniversalClient) xf.Cache {
	return cache{client: client}
}

func (c cache) Get(ctx context.Context, cmd, key string) (interface{}, float64, float64, error) {
	pipe := c.client.Pipeline()
	readPipe := pipe.Do(cmd, key)
	ttlPipe := pipe.Do("PTTL", key) // PTTL returns the time-to-live in milliseconds
	deltaPipe := pipe.Get(fmt.Sprintf("%s:delta", key))

	_, err := pipe.Exec()
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "error on executing pipeline")
	}

	value, err := readPipe.Result()
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "reading")
	}

	ttl, err := ttlPipe.Int64()
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "finding ttl")
	}

	delta, err := deltaPipe.Float64()
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "finding delta")
	}

	return value, float64(ttl) / 1000.0, delta, nil
}

func (c cache) Put(ctx context.Context, cmd, key string, ttl, delta time.Duration, arguments ...interface{}) error {
	pipe := c.client.Pipeline()

	pipe.Do(cmd, args{key}.addFlat(arguments))
	pipe.Expire(key, ttl)
	pipe.MSet(fmt.Sprintf("%s:delta", key), delta.Seconds(), "EX", ttl)

	_, err := pipe.Exec()
	if err != nil {
		return errors.Wrap(err, "sending exec")
	}

	return nil
}

type fieldSpec struct {
	name      string
	index     []int
	omitEmpty bool
}

type structSpec struct {
	m map[string]*fieldSpec
	l []*fieldSpec
}

func (ss *structSpec) fieldSpec(name []byte) *fieldSpec {
	return ss.m[string(name)]
}

func compileStructSpec(t reflect.Type, depth map[string]int, index []int, ss *structSpec) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		switch {
		case f.PkgPath != "" && !f.Anonymous:
			// Ignore unexported fields.
		case f.Anonymous:
			// TODO: Handle pointers. Requires change to decoder and
			// protection against infinite recursion.
			if f.Type.Kind() == reflect.Struct {
				compileStructSpec(f.Type, depth, append(index, i), ss)
			}
		default:
			fs := &fieldSpec{name: f.Name}
			tag := f.Tag.Get("redis")
			p := strings.Split(tag, ",")
			if len(p) > 0 {
				if p[0] == "-" {
					continue
				}
				if len(p[0]) > 0 {
					fs.name = p[0]
				}
				for _, s := range p[1:] {
					switch s {
					case "omitempty":
						fs.omitEmpty = true
					default:
						panic(fmt.Errorf("redigo: unknown field tag %s for type %s", s, t.Name()))
					}
				}
			}
			d, found := depth[fs.name]
			if !found {
				d = 1 << 30
			}
			switch {
			case len(index) == d:
				// At same depth, remove from result.
				delete(ss.m, fs.name)
				j := 0
				for i := 0; i < len(ss.l); i++ {
					if fs.name != ss.l[i].name {
						ss.l[j] = ss.l[i]
						j += 1
					}
				}
				ss.l = ss.l[:j]
			case len(index) < d:
				fs.index = make([]int, len(index)+1)
				copy(fs.index, index)
				fs.index[len(index)] = i
				depth[fs.name] = len(index)
				ss.m[fs.name] = fs
				ss.l = append(ss.l, fs)
			}
		}
	}
}

var (
	structSpecMutex  sync.RWMutex
	structSpecCache  = make(map[reflect.Type]*structSpec)
	defaultFieldSpec = &fieldSpec{}
)

func structSpecForType(t reflect.Type) *structSpec {

	structSpecMutex.RLock()
	ss, found := structSpecCache[t]
	structSpecMutex.RUnlock()
	if found {
		return ss
	}

	structSpecMutex.Lock()
	defer structSpecMutex.Unlock()
	ss, found = structSpecCache[t]
	if found {
		return ss
	}

	ss = &structSpec{m: make(map[string]*fieldSpec)}
	compileStructSpec(t, make(map[string]int), nil, ss)
	structSpecCache[t] = ss
	return ss
}

// Args is a helper for constructing command arguments from structured values.
type args []interface{}

// AddFlat returns the result of appending the flattened value of v to args.
//
// Maps are flattened by appending the alternating keys and map values to args.
//
// Slices are flattened by appending the slice elements to args.
//
// Structs are flattened by appending the alternating names and values of
// exported fields to args. If v is a nil struct pointer, then nothing is
// appended. The 'redis' field tag overrides struct field names. See ScanStruct
// for more information on the use of the 'redis' field tag.
//
// Other types are appended to args as is.
func (args args) addFlat(v interface{}) args {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Struct:
		args = flattenStruct(args, rv)
	case reflect.Slice:
		for i := 0; i < rv.Len(); i++ {
			args = append(args, rv.Index(i).Interface())
		}
	case reflect.Map:
		for _, k := range rv.MapKeys() {
			args = append(args, k.Interface(), rv.MapIndex(k).Interface())
		}
	case reflect.Ptr:
		if rv.Type().Elem().Kind() == reflect.Struct {
			if !rv.IsNil() {
				args = flattenStruct(args, rv.Elem())
			}
		} else {
			args = append(args, v)
		}
	default:
		args = append(args, v)
	}
	return args
}

func flattenStruct(args args, v reflect.Value) args {
	ss := structSpecForType(v.Type())
	for _, fs := range ss.l {
		fv := v.FieldByIndex(fs.index)
		if fs.omitEmpty {
			var empty = false
			switch fv.Kind() {
			case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
				empty = fv.Len() == 0
			case reflect.Bool:
				empty = !fv.Bool()
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				empty = fv.Int() == 0
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				empty = fv.Uint() == 0
			case reflect.Float32, reflect.Float64:
				empty = fv.Float() == 0
			case reflect.Interface, reflect.Ptr:
				empty = fv.IsNil()
			}
			if empty {
				continue
			}
		}
		args = append(args, fs.name, fv.Interface())
	}
	return args
}
