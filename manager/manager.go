package manager

import (
	"context"
	"encoding/json"
	"fmt"
	consul "github.com/hashicorp/consul/api"
	"github.com/vlorc/hprose-gateway-types"
	"net"
	"net/url"
	"sync"
	"time"
	"unsafe"
)

type consulManager struct {
	client  func() *consul.Client
	backend context.Context
	cancel  context.CancelFunc
	scheme  string
	pool    sync.Map
	ttl     int64
}

func NewManager(cli func() *consul.Client, parent context.Context, scheme string, ttl int64) types.NamedManger {
	ctx, cancel := context.WithCancel(parent)
	return &consulManager{
		client:  cli,
		backend: ctx,
		cancel:  cancel,
		scheme:  scheme,
		ttl:     ttl,
	}
}

func (m *consulManager) Register(name, uuid string) types.NamedRegister {
	return m.register(m.formatKey(name, uuid))
}

func (m *consulManager) formatKey(name, uuid string) string {
	if "" != uuid {
		return name + ":" + uuid
	}
	return name
}

func (m *consulManager) register(key string) types.NamedRegister {
	return &consulRegister{
		manager: m,
		key:     key,
	}
}

func (m *consulManager) __toAgentService(key string, val *types.Service) *consul.AgentServiceRegistration {
	u, _ := url.Parse(val.Url)
	addr, port, _ := net.SplitHostPort(u.Host)
	service := &consul.AgentServiceRegistration{
		ID:      key,
		Name:    val.Name,
		Address: addr,
		Tags:    []string{m.scheme},
		Port:    int(types.Integer(port, 0)),
		Check: &consul.AgentServiceCheck{
			CheckID:                        key,
			DeregisterCriticalServiceAfter: fmt.Sprintf("%ds", m.ttl*10),
			TTL:                            fmt.Sprintf("%ds", m.ttl),
			Status:                         "passing",
		},
	}
	service.Meta = make(map[string]string, 8)
	service.Meta["url"] = val.Url
	service.Meta["id"] = val.Id
	service.Meta["path"] = val.Path
	service.Meta["driver"] = val.Driver
	service.Meta["version"] = val.Version
	service.Meta["platform"] = val.Platform
	if len(val.Plugins) > 0 {
		plugins, _ := json.Marshal(val.Plugins)
		service.Meta["plugins"] = *(*string)(unsafe.Pointer(&plugins))
	}
	if len(val.Meta) > 0 {
		meta, _ := json.Marshal(val.Meta)
		service.Meta["meta"] = *(*string)(unsafe.Pointer(&meta))
	}
	return service
}

func (m *consulManager) update(key string, val *types.Service) error {
	service := m.__toAgentService(key, val)
	ticker := time.NewTicker(time.Second * time.Duration(m.ttl-1))
	go func() {
		defer m.client().Agent().ServiceDeregister(key)
		for {
			select {
			case _, ok := <-ticker.C:
				if !ok {
					return
				}
				if err := m.client().Agent().PassTTL(key, ""); nil != err {
					println(err.Error())
				}
			case <-m.backend.Done():
				return
			}
		}
	}()
	if err := m.client().Agent().ServiceRegister(service); err != nil {
		ticker.Stop()
		return err
	}
	m.pool.Store(key, ticker)
	return nil
}

func (m *consulManager) remove(key string) error {
	val, ok := m.pool.Load(key)
	m.pool.Delete(key)
	if ok {
		val.(*time.Ticker).Stop()
	}
	return nil
}

func (m *consulManager) Close() error {
	m.pool.Range(func(key, value interface{}) bool {
		value.(*time.Ticker).Stop()
		return true
	})
	m.pool = sync.Map{}
	return nil
}

func (m *consulManager) Keys() (result []string) {
	m.pool.Range(func(key, _ interface{}) bool {
		result = append(result, key.(string))
		return true
	})
	return result
}
