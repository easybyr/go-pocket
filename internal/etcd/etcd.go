package etcd

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var cli *clientv3.Client

type LockHandler func()

func Init(endpoints []string) {
	var err error
	cli, err = clientv3.New(
		clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: 5 * time.Second,
		},
	)
	if err != nil {
		panic(err)
	}
}

func Close() {
	if cli != nil {
		cli.Close()
	}
}

func Put(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := cli.Put(ctx, key, value)
	cancel()
	if err != nil {
		return err
	}
	return nil
}

func Get(key string) string {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		return ""
	}
	for _, kv := range resp.Kvs {
		if string(kv.Key) == key {
			return string(kv.Value)
		}
	}
	return ""
}

func Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Delete(ctx, key)
	cancel()
	if err != nil {
		return err
	}
	if resp.Deleted != 1 {
		return fmt.Errorf("delete key[%s] error", key)
	}
	return nil
}

func PutEx(key, value string, ttl int64) error {
	// ttl: time in second
	leaseResp, err := cli.Grant(context.TODO(), ttl)
	if err != nil {
		return err
	}
	_, err = cli.Put(context.TODO(), key, value, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return err
	}
	return nil
}

func WithLock(key string, ttl int, handler LockHandler) error {
	session, err := concurrency.NewSession(cli, concurrency.WithTTL(ttl))
	if err != nil {
		return err
	}
	defer session.Close()
	locker := concurrency.NewLocker(session, key)
	locker.Lock()
	handler()
	locker.Unlock()
	return nil
}
