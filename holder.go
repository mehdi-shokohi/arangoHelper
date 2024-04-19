package arangoHelper

import (
	"sync"

	driver "github.com/arangodb/go-driver"
)
type ConnManager struct {
	holder map[string]driver.Client
	mutex sync.RWMutex
}


func (d *ConnManager) read(key string) (driver.Client, bool) {
	d.mutex.RLock()

	defer d.mutex.RUnlock()
	val, exists := d.holder[key]
	return val, exists
}

func (d *ConnManager) write(key string, value driver.Client) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.holder[key] = value
}

func (d *ConnManager) delete(key string)  {
	d.mutex.RLock()

	defer d.mutex.RUnlock()
	delete(d.holder,key)
}
