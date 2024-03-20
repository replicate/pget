package download

// priorityWorkQueue takes work items and executes them, with n parallel
// workers.  It allows for a simple high/low priority split between work.  We
// use this to prefer finishing existing downloads over starting new downloads.
//
// work items are provided with a fixed-size buffer.
type priorityWorkQueue struct {
	concurrency  int
	lowPriority  chan work
	highPriority chan work
	bufSize      int64
}

type work func([]byte)

func newWorkQueue(concurrency int, bufSize int64) *priorityWorkQueue {
	return &priorityWorkQueue{
		concurrency:  concurrency,
		lowPriority:  make(chan work),
		highPriority: make(chan work),
		bufSize:      bufSize,
	}
}

func (q *priorityWorkQueue) submitLow(w work) {
	q.lowPriority <- w
}

func (q *priorityWorkQueue) submitHigh(w work) {
	q.highPriority <- w
}

func (q *priorityWorkQueue) start() {
	for i := 0; i < q.concurrency; i++ {
		go q.run(make([]byte, 0, q.bufSize))
	}
}

func (q *priorityWorkQueue) run(buf []byte) {
	for {
		// read items off the high priority queue until it's empty
		select {
		case item := <-q.highPriority:
			item(buf)
		default:
			select { // read one item from either queue, then go round the loop again
			case item := <-q.highPriority:
				item(buf)
			case item := <-q.lowPriority:
				item(buf)
			}
		}
	}
}
