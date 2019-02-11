package manager

import "github.com/vlorc/hprose-gateway-types"

type consulRegister struct {
	manager *consulManager
	key     string
}

func (r *consulRegister) Update(service *types.Service) error {
	return r.manager.update(r.key, service)
}

func (r *consulRegister) Close() error {
	return r.manager.remove(r.key)
}
