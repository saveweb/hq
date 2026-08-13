package projectstats

import "sync"

const windowSeconds = int64(60)

type bucket struct {
	second   int64
	claimed  int64
	complete int64
}

type Counter struct {
	mu       sync.Mutex
	projects map[string]*[windowSeconds]bucket
}

type Snapshot struct {
	Claimed10, Claimed60     int64
	Completed10, Completed60 int64
}

func New() *Counter { return &Counter{projects: make(map[string]*[windowSeconds]bucket)} }

func (c *Counter) AddClaimed(projectID string, second int64, count int) {
	c.add(projectID, second, int64(count), 0)
}

func (c *Counter) AddCompleted(projectID string, second, count int64) {
	c.add(projectID, second, 0, count)
}

func (c *Counter) add(projectID string, second, claimed, completed int64) {
	if claimed <= 0 && completed <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	buckets := c.projects[projectID]
	if buckets == nil {
		buckets = new([windowSeconds]bucket)
		c.projects[projectID] = buckets
	}
	b := &buckets[second%windowSeconds]
	if b.second != second {
		*b = bucket{second: second}
	}
	b.claimed += claimed
	b.complete += completed
}

func (c *Counter) Snapshot(projectID string, now int64) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result Snapshot
	for _, b := range c.projects[projectID] {
		age := now - b.second
		if age < 0 || age >= windowSeconds {
			continue
		}
		result.Claimed60 += b.claimed
		result.Completed60 += b.complete
		if age < 10 {
			result.Claimed10 += b.claimed
			result.Completed10 += b.complete
		}
	}
	return result
}
