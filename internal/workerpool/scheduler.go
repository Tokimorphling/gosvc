package workerpool

// scheduler stores every pending task in one bounded, preallocated arena.
// Queue links are indices, with zero meaning empty. The ready queue and each
// serial key's waiting queue share these slots, so admission, dispatch and
// completing a serial task are O(1), with no per-task queue allocation.
// The owning Pool holds its mutex for every operation.
type scheduler[K comparable] struct {
	slots   []taskSlot[K]
	free    int
	ready   indexQueue
	serial  map[K]indexQueue // presence reserves the key until its active task ends
	pending int
}

type task[K comparable] struct {
	run    func()
	key    K
	serial bool
}

type taskSlot[K comparable] struct {
	task task[K]
	next int
}

type indexQueue struct{ head, tail int }

func newScheduler[K comparable](capacity int) scheduler[K] {
	s := scheduler[K]{slots: make([]taskSlot[K], capacity+1), free: 1, serial: make(map[K]indexQueue)}
	for i := 1; i < capacity; i++ {
		s.slots[i].next = i + 1
	}
	return s
}

func (s *scheduler[K]) push(run func(), key K, serial bool) bool {
	if s.free == 0 {
		return false
	}
	index := s.free
	s.free = s.slots[index].next
	s.slots[index] = taskSlot[K]{task: task[K]{run: run, key: key, serial: serial}}
	if serial {
		if queue, reserved := s.serial[key]; reserved {
			s.pushBack(&queue, index)
			s.serial[key] = queue
		} else {
			s.serial[key] = indexQueue{}
			s.pushBack(&s.ready, index)
		}
	} else {
		s.pushBack(&s.ready, index)
	}
	s.pending++
	return true
}

func (s *scheduler[K]) pop() (task[K], bool) {
	index := s.popFront(&s.ready)
	if index == 0 {
		return task[K]{}, false
	}
	value := s.slots[index].task
	// Drop references to closures and connection keys as soon as the worker
	// takes ownership, and make this pending slot available to submitters.
	s.slots[index] = taskSlot[K]{next: s.free}
	s.free = index
	s.pending--
	return value, true
}

// complete makes just the next task for this key eligible. Other keys never
// occupy workers while waiting for their predecessor, and get a fair turn on
// the shared ready queue.
func (s *scheduler[K]) complete(completed task[K]) bool {
	if !completed.serial {
		return false
	}
	queue := s.serial[completed.key]
	index := s.popFront(&queue)
	if index == 0 {
		delete(s.serial, completed.key)
		return false
	}
	s.serial[completed.key] = queue
	s.pushBack(&s.ready, index)
	return true
}

func (s *scheduler[K]) pushBack(queue *indexQueue, index int) {
	if queue.tail == 0 {
		queue.head = index
	} else {
		s.slots[queue.tail].next = index
	}
	queue.tail = index
}

func (s *scheduler[K]) popFront(queue *indexQueue) int {
	index := queue.head
	if index != 0 {
		queue.head = s.slots[index].next
		if queue.head == 0 {
			queue.tail = 0
		}
		s.slots[index].next = 0
	}
	return index
}
