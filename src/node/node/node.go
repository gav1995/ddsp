package node

import (
	"sync"
	"time"

	router "router/client"
	"storage"
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
	store sync.Map
	sync.Mutex
	fHeartbeat bool
	changeLock sync.Mutex
	conf       Config
}

// New creates a new Node with a given cfg.
//
// New создает новый Node с данным cfg.
func New(cfg Config) *Node {
	// TODO: implement
	return &Node{
		fHeartbeat: true,
		conf:       cfg,
	}
}

// Hearbeats runs heartbeats from node to a router
// each time interval set by cfg.Hearbeat.
//
// Hearbeats запускает отправку heartbeats от node к router
// через каждый интервал времени, заданный в cfg.Heartbeat.
func (node *Node) Heartbeats() {
	go func() {
		for {
			node.Lock()
			if node.fHeartbeat == false {
				node.Unlock()
				break
			}
			node.conf.Client.Heartbeat(node.conf.Router, node.conf.Addr)
			node.Unlock()

			time.Sleep(node.conf.Heartbeat)
		}
	}()
	return
}

// Stop stops heartbeats
//
// Stop останавливает отправку heartbeats.
func (node *Node) Stop() {
	node.Lock()
	node.fHeartbeat = false
	node.Unlock()
	return
}

// Put an item to the node if an item for the given key doesn't exist.
// Returns the storage.ErrRecordExists error otherwise.
//
// Put -- добавить запись в node, если запись для данного ключа
// не существует. Иначе вернуть ошибку storage.ErrRecordExists.
func (node *Node) Put(k storage.RecordID, d []byte) error {
	node.changeLock.Lock()
	_, load := node.store.LoadOrStore(k, d)
	node.changeLock.Unlock()
	if load == true {
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
	node.changeLock.Lock()
	defer node.changeLock.Unlock()

	_, ok := node.store.Load(k)
	if ok == true {
		node.store.Delete(k)
		return nil
	}
	return storage.ErrRecordNotFound
}

// Get an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Get -- получить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Get(k storage.RecordID) ([]byte, error) {
	item, ok := node.store.Load(k)
	if ok {
		return item.([]byte), nil
	}

	return nil, storage.ErrRecordNotFound

}
