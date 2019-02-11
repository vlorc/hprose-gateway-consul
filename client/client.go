package client

import (
	consul "github.com/hashicorp/consul/api"
	"net/url"
	"sync"
	"sync/atomic"
)

func NewClient(addr string) func() *consul.Client {
	return func() *consul.Client {
		config := consul.DefaultConfig()
		if u,_ := url.Parse(addr); nil != u {
			config.Scheme = u.Scheme
			config.Address = u.Host
		} else if "" != addr{
			config.Address = addr
		}

		cli, err := consul.NewClient(config)
		if nil != err {
			panic(err)
		}
		return cli
	}
}

func NewLazyClient(client func() *consul.Client) func() *consul.Client {
	var once sync.Once
	var value atomic.Value
	return func() *consul.Client {
		once.Do(func() {
			cli := client()
			client = nil
			value.Store(cli)
		})
		return value.Load().(*consul.Client)
	}
}