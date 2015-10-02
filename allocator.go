package dropsite

import (
	"sync"
	"time"
)

type AllocationConfig struct {
	// MaxDifferential is the maximum difference in data usage between the least and most utilized
	// drop sites.
	MaxDifferential int64

	// MaxErrorTimeout is the maximum amount of time that an error-causing drop site should be
	// disabled.
	MaxErrorTimeout time.Duration
}

type dropSiteUsage struct {
	index int
	usage int64

	consecutiveErrors int64
}

// An allocator determines which drop sites get used and how often.
//
// The allocator attempts to minimize the difference in data usage between the least and most
// utilized drop sites.
//
// The Allocator temporarily disables drop sites which cause errors.
type allocator struct {
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
			newList := make([]timeoutListEntry, len(list)+1)
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
		res = append(res, t.list[i])
	}
	if len(res) > 0 {
		t.list = make([]*dropSiteUsage, len(t.list)-len(res))
		copy(t.list, t.list[len(res):])
	}
	return res
}

func (t *timeoutList) popAll() []*dropSiteUsage {
	res := t.list
	t.list = nil
	return res
}
