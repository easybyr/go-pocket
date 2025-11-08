package etcd_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/easybyr/go-pocket/internal/etcd"
)

func TestPutAndGet(t *testing.T) {
	endpoints := []string{"127.0.0.1:2379"}
	etcd.Init(endpoints)
	defer etcd.Close()
	key := "中国"
	etcd.Put(key, "国家")

	value := etcd.Get(key)
	fmt.Printf("get value from etcd, value:%v\n", value)

	etcd.Delete(key)
}

func TestLock(t *testing.T) {
	etcd.Init([]string{"127.0.0.1:2379"})
	key := "testKey"

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			etcd.WithLock(key, 5, func() {
				time.Sleep(1 * time.Second)
				fmt.Printf("work node-%d\n", id)
			})
		}(i)
	}
	wg.Wait()
	fmt.Println("finished!")
}
