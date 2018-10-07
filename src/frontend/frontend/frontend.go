package frontend

import (
	"time"
	"sync"

	rclient "router/client"
	"router/router"
	"storage"
)

// InitTimeout is a timeout to wait after unsuccessful List() request to Router.
//
// InitTimeout -- количество времени, которое нужно подождать до следующей попытки
// отправки запроса List() в Router.
const InitTimeout = 100 * time.Millisecond

// Config stores configuration for a Frontend service.
//
// Config -- содержит конфигурацию Frontend.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Frontend.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr

	// NC specifies client for Node.
	// NC -- клиент для node.
	NC storage.Client `yaml:"-"`
	// RC specifies client for Router.
	// RC -- клиент для router.
	RC rclient.Client `yaml:"-"`
	// NodesFinder specifies a NodeFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Frontend.
	NF router.NodesFinder `yaml:"-"`
}

// Frontend is a frontend service.
type Frontend struct {
	sync.RWMutex
	conf Config

}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	return &Frontend{conf:cfg,}
}

// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
//
// Put -- добавить запись в хранилище, если запись для данного ключа
// не существует. Иначе вернуть ошибку.
func (fe *Frontend) Put(k storage.RecordID, d []byte) error {
	fe.Lock()
	defer fe.Unlock()
	nodes, err:=fe.conf.RC.NodesFind(fe.conf.Router,k)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	good := 0
	var mu sync.Mutex
	wg.Add(len(nodes))
	for i := range(nodes){
		go func(){
			defer wg.Done()
			err1 := fe.conf.NC.Put(nodes[i],k, d)
			if(err1 != nil){
				err = err1
			} else {
				mu.Lock()
				good ++
				mu.Unlock()
			}
			
		}()
	}

	wg.Wait()
	return err
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	fe.Lock()
	defer fe.Unlock()
	nodes, err:=fe.conf.RC.NodesFind(fe.conf.Router,k)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var good = 0
	var mu sync.Mutex
	wg.Add(len(nodes))
	for i := range(nodes){
		go func(){
			defer wg.Done()
			err1 := fe.conf.NC.Del(nodes[i],k)
			if(err1 != nil){
				err = err1
			} else {
				mu.Lock()
				good ++
				mu.Unlock()
			}
			
		}()
	}

	wg.Wait()
	
	return err
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	// TODO: implement
	return nil, nil
}
