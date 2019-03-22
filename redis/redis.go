package redis

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/mytokenio/go/log"
)

var (
	mutex    sync.Mutex
	pool     *redis.Pool
	poolMaps map[string]*redis.Pool
)

const (
	lockSuffix      = "_lock"
	defaultInstance = "main"
)

type RdsCfg struct {
	Instance  string // instance name
	Address   string // redis address(ip:port)
	DB        int    // use db
	Timeout   int    // connect timeout
	Auth      string // redis auth
	IsDefault bool   // is default instance
}

// ---------------------------------------------------------------------------------------------------------------------

func init() {
	poolMaps = make(map[string]*redis.Pool)
}

func Init(address string, db, timeout int, auth string, instance ...string) error {
	mutex.Lock()
	defer mutex.Unlock()

	var instanceName string

	instanceLen := len(instance)
	if instanceLen == 0 {
		instanceName = defaultInstance
	} else if instanceLen == 1 {
		if instance[0] == "" {
			instanceName = defaultInstance
		} else {
			instanceName = instance[0]
		}
	} else {
		return fmt.Errorf("params error, instance invalid")
	}

	if _, ok := poolMaps[instanceName]; ok {
		return nil
	}

	if p, err := newRedisPool(address, db, timeout, auth); err != nil {
		return err
	} else {
		poolMaps[instanceName] = p
		if len(poolMaps) == 1 && pool == nil {
			pool = p
		}
	}

	return nil
}

func BatchInit(redisCfg []*RdsCfg) error {
	mutex.Lock()
	defer mutex.Unlock()

	for _, r := range redisCfg {
		if r.Instance == "" {
			return fmt.Errorf("params error, instance invalid")
		}
		if _, ok := poolMaps[r.Instance]; !ok {
			p, err := newRedisPool(r.Address, r.DB, r.Timeout, r.Auth)
			if err != nil {
				return err
			}
			poolMaps[r.Instance] = p
			if r.IsDefault && pool == nil {
				pool = p
			}
		}
	}

	return nil
}

func GetInstance(instance ...string) (redis.Conn, error) {
	instanceLen := len(instance)
	if instanceLen == 0 {
		if pool == nil {
			return nil, fmt.Errorf("default instance is not exist")
		} else {
			return pool.Get(), nil
		}
	}

	if instanceLen == 1 {
		mutex.Lock()
		defer mutex.Unlock()
		instanceName := instance[0]
		if instanceName == "" {
			instanceName = defaultInstance
		}
		if p, ok := poolMaps[instanceName]; !ok {
			return nil, fmt.Errorf("instance: %s is not exist", instanceName)
		} else {
			return p.Get(), nil
		}
	}

	return nil, fmt.Errorf("params error, instance invalid")
}

// redis string
// ---------------------------------------------------------------------------------------------------------------------
func Get(key string, instance ...string) (string, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return "", err
	}
	defer c.Close()

	log.Infof("[Redis] GET %v", key)
	return redis.String(c.Do("GET", key))
}

func Set(key string, value interface{}, instance ...string) error {
	c, err := GetInstance(instance...)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Infof("[Redis] SET %v %v", key, value)
	_, err = c.Do("SET", key, value)
	return err
}

func Del(key string, instance ...string) error {
	c, err := GetInstance(instance...)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Infof("[Redis] DEL %v", key)
	_, err = c.Do("DEL", key)
	return err
}

func Keys(pattern string, instance ...string) ([]string, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	log.Infof("[Redis] KEYS %v", pattern)
	return redis.Strings(c.Do("KEYS", pattern))
}

// redis hash
// ---------------------------------------------------------------------------------------------------------------------
func Hget(key string, field interface{}, instance ...string) (string, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return "", err
	}
	defer c.Close()

	return redis.String(c.Do("HGET", key, field))
}

/**
 * 返回值说明：
 *   1. 如果 key 不存在，操作成功则返回 true, nil
 *   2. 如果 key 存在，field 不存在，操作成功则返回 true, nil
 *   3. 如果 key, field 都存在，覆盖旧值，操作成功则返回 false, nil
 */
func Hset(key string, field, value interface{}, instance ...string) (bool, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return false, err
	}
	defer c.Close()

	log.Infof("[Redis] HSET %v %v %v", key, field, value)
	isCovered, err := redis.Bool(c.Do("HSET", key, field, value))
	return isCovered, err
}

func Hdel(key string, field interface{}, instance ...string) error {
	c, err := GetInstance(instance...)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Infof("[Redis] HDEL %v %v", key, field)
	_, err = c.Do("HDEL", key, field)
	return err
}

func Hmset(key string, value interface{}, instance ...string) error {
	c, err := GetInstance(instance...)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Infof("[Redis] HMSET %v %+v", key, value)
	_, err = c.Do("HMSET", redis.Args{}.Add(key).AddFlat(value)...)
	return err
}

func Hgetall(key string, instance ...string) (map[string]string, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	log.Infof("[Redis] HGETALL %v", key)
	reply, err := c.Do("HGETALL", key)
	if err != nil {
		return nil, err
	}
	res, err := bytesSlice(reply)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	writeToContainer(res, reflect.ValueOf(result))

	return result, err
}

// redis list
// ---------------------------------------------------------------------------------------------------------------------
func LRange(key string, start, end int, instance ...string) ([]string, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	log.Infof("[Redis] LRANGE %v %v %v", key, start, end)
	return redis.Strings(c.Do("LRANGE", key, start, end))
}

func Lrem(key string, count int, value interface{}, instance ...string) error {
	c, err := GetInstance(instance...)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Infof("[Redis] LREM %v %v %v", key, count, value)
	_, err = c.Do("LREM", key, count, value)
	return err
}

func Lpush(key string, value interface{}, instance ...string) error {
	c, err := GetInstance(instance...)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Infof("[Redis] LPUSH %v %v", key, value)
	_, err = c.Do("LPUSH", key, value)
	return err
}

func Rpush(key string, value interface{}, instance ...string) error {
	c, err := GetInstance(instance...)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Infof("[Redis] RPUSH %v %v", key, value)
	_, err = c.Do("RPUSH", key, value)
	return err
}

func Rpop(key string, instance ...string) (string, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return "", err
	}
	defer c.Close()

	log.Infof("[Redis] RPOP %v", key)
	return redis.String(c.Do("RPOP", key))
}

func Brpoplpush(source, destination string, timeout int, instance ...string) (string, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return "", err
	}
	defer c.Close()

	log.Infof("[Redis] BRPOPLPUSH %v %v %v", source, destination, timeout)
	return redis.String(c.Do("BRPOPLPUSH", source, destination, timeout))
}

// redis lock
// ---------------------------------------------------------------------------------------------------------------------
func TryLock(key string, milliseconds int, instance ...string) (bool, string, error) {
	c, err := GetInstance(instance...)
	if err != nil {
		return false, "", err
	}
	defer c.Close()

	key = key + lockSuffix
	value := strconv.FormatInt(time.Now().UnixNano()/1000, 10)

	log.Infof("[Redis] TryLock SET %v %v PX %v NX", key, value, milliseconds)

	_, err = redis.String(c.Do("SET", key, value, "PX", milliseconds, "NX"))
	if err == redis.ErrNil {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}

	return true, value, nil
}

func UnLock(key string, instance ...string) error {
	c, err := GetInstance(instance...)
	if err != nil {
		return err
	}
	defer c.Close()

	key = key + lockSuffix
	log.Infof("[Redis] UnLock DEL %v", key)
	_, err = c.Do("DEL", key)
	return err
}

// redis do
// ---------------------------------------------------------------------------------------------------------------------
func Do(cmd string, args ...interface{}) (interface{}, error) {
	if pool == nil {
		return nil, fmt.Errorf("redis pool is nil")
	}

	c := pool.Get()
	defer c.Close()

	return c.Do(cmd, args...)
}

func DoByInstance(instance, cmd string, args ...interface{}) (interface{}, error) {
	c, err := GetInstance(instance)
	if err != nil {
		return "", err
	}
	defer c.Close()

	return c.Do(cmd, args...)
}

// ---------------------------------------------------------------------------------------------------------------------

func newRedisPool(addr string, db, timeout int, auth string) (*redis.Pool, error) {
	options := make([]redis.DialOption, 1)
	options[0] = redis.DialConnectTimeout(time.Duration(timeout) * time.Second)

	redisPool := &redis.Pool{
		MaxIdle:     80,
		MaxActive:   10000,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", addr, options...)
			if err != nil {
				return nil, err
			}
			if auth != "" {
				if _, err = conn.Do("AUTH", auth); err != nil {
					return nil, err
				}
			}
			if _, err = conn.Do("SELECT", db); err != nil {
				return nil, err
			}
			return conn, err
		},
	}

	if conn := redisPool.Get(); conn == nil {
		return nil, fmt.Errorf("can not get new redis conn")
	} else {
		if _, err := redis.String(conn.Do("PING")); err != nil {
			conn.Close()
			return nil, err
		}
	}

	return redisPool, nil
}

func bytesSlice(reply interface{}) ([][]byte, error) {
	switch reply := reply.(type) {
	case []interface{}:
		result := make([][]byte, len(reply))
		for i := range reply {
			if reply[i] == nil {
				continue
			}
			p, ok := reply[i].([]byte)
			if !ok {
				return nil, fmt.Errorf("redigo: unexpected element type for []byte, got type %T", reply[i])
			}
			result[i] = p
		}
		return result, nil
	case nil:
		return nil, redis.ErrNil
	case redis.Error:
		return nil, reply
	}
	return nil, fmt.Errorf("redigo: unexpected type for []byte, got type %T", reply)
}

func writeToContainer(data [][]byte, val reflect.Value) error {
	switch v := val; v.Kind() {
	case reflect.Ptr:
		return writeToContainer(data, reflect.Indirect(v))
	case reflect.Interface:
		return writeToContainer(data, v.Elem())
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("redigo invalid map type")
		}
		elemType := v.Type().Elem()
		for i := 0; i < len(data)/2; i++ {
			mk := reflect.ValueOf(string(data[i*2]))
			mv := reflect.New(elemType).Elem()
			writeTo(data[i*2+1], mv)
			v.SetMapIndex(mk, mv)
		}
	case reflect.Struct:
		for i := 0; i < len(data)/2; i++ {
			name := string(data[i*2])
			field := v.FieldByName(name)
			if !field.IsValid() {
				continue
			}
			writeTo(data[i*2+1], field)
		}
	default:
		return fmt.Errorf("redigo invalid container type")
	}
	return nil
}

func writeTo(data []byte, val reflect.Value) error {
	s := string(data)
	switch v := val; v.Kind() {

	case reflect.Interface:
		v.Set(reflect.ValueOf(data))

	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		v.SetBool(b)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		v.SetInt(i)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		ui, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		v.SetUint(ui)

	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		v.SetFloat(f)

	case reflect.String:
		v.SetString(s)

	case reflect.Slice:
		typ := v.Type()
		if typ.Elem().Kind() == reflect.Uint ||
			typ.Elem().Kind() == reflect.Uint8 ||
			typ.Elem().Kind() == reflect.Uint16 ||
			typ.Elem().Kind() == reflect.Uint32 ||
			typ.Elem().Kind() == reflect.Uint64 ||
			typ.Elem().Kind() == reflect.Uintptr {
			v.Set(reflect.ValueOf(data))
		}
	}
	return nil
}
