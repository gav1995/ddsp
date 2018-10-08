package router

import (
	"sync"
	"time"

	"storage"
)

// Config stores configuration for a Router service.
//
// Config -- содержит конфигурацию Router.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес.
	Addr storage.ServiceAddr

	// Nodes is a list of nodes served by the Router.
	// Nodes -- список node обслуживаемых Router.
	Nodes []storage.ServiceAddr

	// ForgetTimeout is a timeout after node is considered to be unavailable
	// in absence of hearbeats.
	// ForgetTimeout -- если в течении ForgetTimeout node не посылала heartbeats, то
	// node считается недоступной.
	ForgetTimeout time.Duration `yaml:"forget_timeout"`

	// NodesFinder specifies a NodesFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Router.
	NodesFinder NodesFinder `yaml:"-"`
}

// Router is a router service.
type Router struct {
	sync.RWMutex
	list        []storage.ServiceAddr
	nodes       map[storage.ServiceAddr]time.Time
	timeout     time.Duration
	addr        storage.ServiceAddr
	nodesFinder NodesFinder
}

// New creates a new Router with a given cfg.
// Returns storage.ErrNotEnoughDaemons error if less then storage.ReplicationFactor
// nodes was provided in cfg.Nodes.
//
// New создает новый Router с данным cfg.
// Возвращает ошибку storage.ErrNotEnoughDaemons если в cfg.Nodes
// меньше чем storage.ReplicationFactor nodes.
func New(cfg Config) (*Router, error) {
	if len(cfg.Nodes) < storage.ReplicationFactor {
		return nil, storage.ErrNotEnoughDaemons
	}
	rout := Router{
		nodes:       make(map[storage.ServiceAddr]time.Time),
		timeout:     cfg.ForgetTimeout,
		addr:        cfg.Addr,
		nodesFinder: cfg.NodesFinder,
		list:        cfg.Nodes,
	}
	rout.Lock()
	for _, v := range cfg.Nodes {
		rout.nodes[v] = time.Time{}
	}
	rout.Unlock()
	return &rout, nil
}

// Hearbeat registers node in the router.
// Returns storage.ErrUnknownDaemon error if node is not served by the Router.

// Hearbeat регистритрует node в router.
// Возвращает ошибку storage.ErrUnknownDaemon если node не
// обслуживается Router.
func (r *Router) Heartbeat(node storage.ServiceAddr) error {
	r.Lock()
	defer r.Unlock()
	_, ok := r.nodes[node]
	if !ok {
		return storage.ErrUnknownDaemon
	} else {
		r.nodes[node] = time.Now()
	}
	return nil
}

// NodesFind returns a list of available nodes, where record with associated key k
// should be stored. Returns storage.ErrNotEnoughDaemons error
// if less then storage.MinRedundancy can be returned.
//
// NodesFind возвращает cписок достпуных node, на которых должна храниться
// запись с ключом k. Возвращает ошибку storage.ErrNotEnoughDaemons
// если меньше, чем storage.MinRedundancy найдено.
func (r *Router) NodesFind(k storage.RecordID) ([]storage.ServiceAddr, error) {

	nodes := r.nodesFinder.NodesFind(k, r.list)

	var ans []storage.ServiceAddr
	r.RLock()
	defer r.RUnlock()
	for _, v := range nodes {
		if time.Now().Sub(r.nodes[v]) < r.timeout {
			ans = append(ans, v)
		}
	}

	if len(ans) < storage.MinRedundancy {
		return nil, storage.ErrNotEnoughDaemons
	}
	return ans, nil
}

// List returns a list of all nodes served by Router.
//
// List возвращает cписок всех node, обслуживаемых Router.
func (r *Router) List() []storage.ServiceAddr {
	// r.RLock()
	// defer r.RUnlock()
	return r.list
}
