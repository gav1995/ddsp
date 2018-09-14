package node

import (
	router "router/client"
	"storage"
	"sync"
	"time"
)

// Config stores configuration for a Node service.
//
// Config -- содержит конфигурацию Node.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Node.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr
	// Hearbeat is a time interval between hearbeats.
	// Hearbeat -- интервал между двумя heartbeats.
	Heartbeat time.Duration

	// Client specifies client for Router.
	// Client -- клиент для Router.
	Client router.Client `yaml:"-"`
}

// Node is a Node service.
type Node struct {
	// TODO: implement
	Store      sync.Map //[storage.RecordID][]byte
	Mu         sync.Mutex
	FHeartbeat bool
	Conf       Config
}

// New creates a new Node with a given cfg.
//
// New создает новый Node с данным cfg.
func New(cfg Config) *Node {
	// TODO: implement
	return &Node{
		FHeartbeat: true,
		Conf:       cfg,
	}
}

// Hearbeats runs heartbeats from node to a router
// each time interval set by cfg.Hearbeat.
//
// Hearbeats запускает отправку heartbeats от node к router
// через каждый интервал времени, заданный в cfg.Heartbeat.
func (node *Node) Heartbeats() {
	// TODO: implement
	go func() {
		for {
			node.Mu.Lock()
			if node.FHeartbeat == false {
				node.Mu.Unlock()
				break
			}
			node.Conf.Client.Heartbeat(node.Conf.Router, node.Conf.Addr)
			node.Mu.Unlock()

			time.Sleep(node.Conf.Heartbeat)
		}
	}()
	return
}

// Stop stops heartbeats
//
// Stop останавливает отправку heartbeats.
func (node *Node) Stop() {
	// TODO: implement
	node.Mu.Lock()
	node.FHeartbeat = false
	node.Mu.Unlock()
	return
}

// Put an item to the node if an item for the given key doesn't exist.
// Returns the storage.ErrRecordExists error otherwise.
//
// Put -- добавить запись в node, если запись для данного ключа
// не существует. Иначе вернуть ошибку storage.ErrRecordExists.
func (node *Node) Put(k storage.RecordID, d []byte) error {
	// TODO: implement
	_, isLoad := node.Store.LoadOrStore(k, d)
	if isLoad == true {
		return storage.ErrRecordExists
	}
	return nil
}

// Del an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Del -- удалить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Del(k storage.RecordID) error {
	// TODO: implement
	_, isLoad := node.Store.Load(k)
	if isLoad == true {
		node.Store.Delete(k)
	} else {
		return storage.ErrRecordNotFound
	}
	return nil
}

// Get an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Get -- получить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Get(k storage.RecordID) ([]byte, error) {
	// TODO: implement
	item, isLoad := node.Store.Load(k)
	if isLoad {
		return item.([]byte), nil
	} else {
		return nil, storage.ErrRecordNotFound
	}
	return nil, nil
}
