package resolver

import (
	"context"
	"encoding/json"
	consul "github.com/hashicorp/consul/api"
	"github.com/vlorc/hprose-gateway-types"
	"net"
	"strconv"
	"unsafe"
)

type consulResolver struct {
	client  func() *consul.Client
	backend context.Context
	cancel  context.CancelFunc
	scheme  string
	index   uint64
	table   map[string]uint64
}

func NewResolver(client func() *consul.Client, parent context.Context, scheme string) types.NamedResolver {
	ctx, cancel := context.WithCancel(parent)
	return &consulResolver{
		client:  client,
		backend: ctx,
		cancel:  cancel,
		scheme:  scheme,
		table: make(map[string]uint64),
	}
}

func (r *consulResolver) Close() error {
	r.cancel()
	return nil
}

func (r *consulResolver) Watch(name string, watcher types.NamedWatcher) error {
	r.next(name, watcher)
	r.watch(name, watcher)
	return nil
}

func (r *consulResolver) watch(name string, watcher types.NamedWatcher) {
	for {
		r.next(name, watcher)
	}
	return
}

func (r *consulResolver) All(name string, watcher types.NamedWatcher) error {
	return r.next(name, watcher)
}

func (r *consulResolver) next(name string, watcher types.NamedWatcher) error {
	service, meta, err := r.client().Health().Service(name, r.scheme, true, (&consul.QueryOptions{
		WaitIndex: r.index,
	}).WithContext(r.backend))
	if err != nil {
		return err
	}
	r.index = meta.LastIndex
	if updates := r.extract(name, service); len(updates) > 0 {
		watcher.Push(updates)
	}
	return nil
}

func (r *consulResolver) __toService(val *consul.ServiceEntry) (service *types.Service) {
	service = &types.Service{
		Id:       val.Service.Meta["id"],
		Name:     val.Service.Service,
		Path:     val.Service.Meta["path"],
		Driver:   val.Service.Meta["driver"],
		Version:  val.Service.Meta["version"],
		Url:      val.Service.Meta["url"],
		Platform: val.Service.Meta["platform"],
	}
	if "" == service.Url {
		service.Url = net.JoinHostPort(val.Service.Address, strconv.Itoa(val.Service.Port))
	}
	if plugins, ok := val.Service.Meta["plugins"]; ok && "null" != plugins {
		json.Unmarshal(*(*[]byte)(unsafe.Pointer(&plugins)), &service.Plugins)
	}
	if meta, ok := val.Service.Meta["meta"]; ok && "null" != meta {
		json.Unmarshal(*(*[]byte)(unsafe.Pointer(&meta)), &service.Meta)
	}
	return
}

func __exsit(id string, service []*consul.ServiceEntry) bool{
	for _,s := range service {
		if s.Service.ID == id {
			return true
		}
	}
	return false
}

func (r *consulResolver) extract(prefix string, service []*consul.ServiceEntry) (result []types.Update) {
	if service == nil {
		return
	}
	result = make([]types.Update, 0, len(service))
	for k := range r.table {
		if !__exsit(k,service) {
			delete(r.table,k)
			result = append(result, types.Update{
				Op:      types.Delete,
				Id:      k,
			})
		}
	}
	for _, s := range service {
		info := r.__toService(s)
		if index,ok := r.table[s.Service.ID]; ok && index == s.Service.ModifyIndex{
			continue
		}
		r.table[s.Service.ID] = s.Service.ModifyIndex
		if nil != info {
			result = append(result, types.Update{
				Op:      types.Add,
				Id:      s.Service.ID,
				Service: info,
			})
		}
	}
	return
}
