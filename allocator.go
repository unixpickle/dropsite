package dropsite

import (
	"sync"
	"time"
)

type dropSiteUsage struct {
	id    int
	usage int64
	inUse bool

	consecutiveErrors uint32
}

// An allocator determines which drop sites get used and how often.
//
// An allocator should attempt to minimize the difference in data usage between the least and most
// utilized drop sites.
//
// An allocator temporarily disables drop sites which cause errors.
type allocator struct {
	maxDisableTimeout time.Duration

	lock sync.Mutex

	timeouts  *timeoutList
	dropSites []*dropSiteUsage
}

func newAllocator(maxDisableTimeout time.Duration, count int) *allocator {
	var res allocator

	res.maxDisableTimeout = maxDisableTimeout
	res.timeouts = &timeoutList{}
	res.dropSites = make([]*dropSiteUsage, count)
	for i := 0; i < count; i++ {
		res.dropSites[i] = &dropSiteUsage{id: i}
	}

	return &res
}

// Alloc allocates a drop site and returns its ID.
func (a *allocator) Alloc() int {
	a.lock.Lock()
	defer a.lock.Unlock()

	var min int64
	foundId := -1
	for _, ds := range a.dropSites {
		if ds.inUse {
			continue
		}
		if ds.usage < min || foundId == -1 {
			min = ds.usage
			foundId = ds.id
		}
	}

	if foundId < 0 {
		badDs := a.timeouts.popOne()
		a.dropSites = append(a.dropSites, badDs)
		foundId = badDs.id
	}

	return foundId
}

// Free deallocates a drop site, allowing it to be re-used for later allocations.
func (a *allocator) Free(id int, usage int64) {
	a.lock.Lock()
	defer a.lock.Unlock()

	for _, ds := range a.dropSites {
		if ds.id == id {
			ds.usage += usage
			ds.consecutiveErrors = 0
			ds.inUse = false
			return
		}
	}

	panic("unknown ID")
}

// Failed deallocates a drop site, potentially disabling it as a penalty for its error.
func (a *allocator) Failed(id int) {
	a.lock.Lock()
	defer a.lock.Unlock()

	for index, ds := range a.dropSites {
		if ds.id == id {
			ds.inUse = false
			if ds.consecutiveErrors < 31 {
				ds.consecutiveErrors++
			}

			timeout := time.Second * time.Duration(1<<ds.consecutiveErrors)
			if timeout > a.maxDisableTimeout {
				timeout = a.maxDisableTimeout
			}
			a.timeouts.push(ds, timeout)

			copy(a.dropSites[index:], a.dropSites[index+1:])
			a.dropSites = a.dropSites[:len(a.dropSites)-1]
			return
		}
	}

	panic("unknown ID")
}

type timeoutListEntry struct {
	dsu         *dropSiteUsage
	endUnixTime int64
}

type timeoutList struct {
	list []timeoutListEntry
}

func (t *timeoutList) push(dsu *dropSiteUsage, timeout time.Duration) {
	endTime := time.Now().Add(timeout).Unix()
	entry := timeoutListEntry{dsu, endTime}
	for i := 0; i < len(t.list); i++ {
		if endTime < t.list[i].endUnixTime {
			newList := make([]timeoutListEntry, len(t.list)+1)
			copy(newList, t.list[:i])
			newList[i] = entry
			copy(newList[i+1:], t.list[i:])
			t.list = newList
			return
		}
	}
	t.list = append(t.list, entry)
}

func (t *timeoutList) popTimedOut() []*dropSiteUsage {
	var res []*dropSiteUsage
	now := time.Now().Unix()
	for i := 0; i < len(t.list); i++ {
		if t.list[i].endUnixTime >= now {
			break
		}
		res = append(res, t.list[i].dsu)
	}
	if len(res) > 0 {
		copy(t.list, t.list[len(res):])
		t.list = t.list[:len(t.list)-len(res)]
	}
	return res
}

func (t *timeoutList) popOne() *dropSiteUsage {
	if len(t.list) == 0 {
		panic("cannot pop anything")
	}
	res := t.list[0]
	copy(t.list, t.list[1:])
	t.list = t.list[:len(t.list)-1]
	return res.dsu
}
