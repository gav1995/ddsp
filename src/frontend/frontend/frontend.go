package frontend

import (
	"sync"
	"time"

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
	conf  Config
	nodes []storage.ServiceAddr
	once  sync.Once
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	return &Frontend{conf: cfg}
}

func (fe *Frontend) putDel(k storage.RecordID, do func(node storage.ServiceAddr) error) error {
	fe.Lock()
	defer fe.Unlock()
	nodes, err := fe.conf.RC.NodesFind(fe.conf.Router, k)

	if err != nil {
		return err
	}

	if len(nodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make(map[error]int)
	var good int = 0
	wg.Add(len(nodes))

	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			defer wg.Done()
			err1 := do(node)

			mu.Lock()
			defer mu.Unlock()
			if err1 == nil {
				good++
			} else {
				errors[err1]++
			}

		}(node)
	}
	wg.Wait()

	for err, num := range errors {
		if num >= storage.MinRedundancy {
			return err
		}
	}

	if good >= storage.MinRedundancy {
		return nil
	}

	return storage.ErrQuorumNotReached

}

// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
func (fe *Frontend) Put(k storage.RecordID, d []byte) error {
	return fe.putDel(k, func(node storage.ServiceAddr) error {
		return fe.conf.NC.Put(node, k, d)
	})
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	return fe.putDel(k, func(node storage.ServiceAddr) error {
		return fe.conf.NC.Del(node, k)
	})
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	fe.once.Do(func() {
		var err error
		fe.nodes, err = fe.conf.RC.List(fe.conf.Router)
		for err != nil {
			time.Sleep(InitTimeout)
			fe.nodes, err = fe.conf.RC.List(fe.conf.Router)
		}
	})

	nodes := fe.conf.NF.NodesFind(k, fe.nodes)

	if len(nodes) < storage.MinRedundancy {
		return nil, storage.ErrNotEnoughDaemons
	}

	values := make(map[string]int)
	err := make(map[error]int)

	type pair struct {
		val []byte
		err error
	}

	ch := make(chan pair, len(nodes))
	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			val, err := fe.conf.NC.Get(node, k)
			ch <- pair{val, err}
		}(node)
	}
	for i := 0; i < storage.ReplicationFactor; i++ {
		ans := <-ch
		if ans.err != nil {
			err[ans.err]++
			if err[ans.err] == storage.MinRedundancy {
				return nil, ans.err
			}
		} else {
			values[string(ans.val)]++
			if values[string(ans.val)] == storage.MinRedundancy {
				return ans.val, nil
			}
		}
	}

	return nil, storage.ErrQuorumNotReached
}
