package proxyables

import (
	"reflect"
	"sync"
)

// ObjectRegistry tracks objects and their reference counts.
type ObjectRegistry struct {
	mu      sync.Mutex
	objects map[string]interface{}
	counts  map[string]int
	reverse map[uintptr]string
}

func NewObjectRegistry() *ObjectRegistry {
	return &ObjectRegistry{
		objects: make(map[string]interface{}),
		counts:  make(map[string]int),
		reverse: make(map[uintptr]string),
	}
}

func pointerKey(value interface{}) uintptr {
	if value == nil {
		return 0
	}
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return 0
	}
	kind := v.Kind()
	switch kind {
	case reflect.Ptr, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return v.Pointer()
	default:
		return 0
	}
}

func (r *ObjectRegistry) Register(value interface{}) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := pointerKey(value)
	if key != 0 {
		if existing, ok := r.reverse[key]; ok {
			r.counts[existing] = r.counts[existing] + 1
			return existing
		}
	}

	id := MakeID()
	r.objects[id] = value
	r.counts[id] = 1
	if key != 0 {
		r.reverse[key] = id
	}
	return id
}

func (r *ObjectRegistry) Get(id string) (interface{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.objects[id]
	return value, ok
}

func (r *ObjectRegistry) Delete(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := r.counts[id] - 1
	if count <= 0 {
		if value, ok := r.objects[id]; ok {
			key := pointerKey(value)
			if key != 0 {
				delete(r.reverse, key)
			}
		}
		delete(r.objects, id)
		delete(r.counts, id)
		return
	}
	r.counts[id] = count
}

func (r *ObjectRegistry) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.objects)
}
