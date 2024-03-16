package download

// workQueue takes work items and executes them serially, with n concurrent
// workers.  It allows for a simple high/low priority split between work.  We
// use this to prefer finishing existing downloads over starting new downloads.
type workQueue struct {
	concurrency  int
	lowPriority  chan work
	highPriority chan work
}

type work func()

// What's a good number here?
var depth = 100

func newWorkQueue(concurrency int) *workQueue {
	return &workQueue{
		concurrency:  concurrency,
		lowPriority:  make(chan work, concurrency),
		highPriority: make(chan work, depth),
	}
}

func (q *workQueue) submitLow(w work) {
	q.lowPriority <- w
}

func (q *workQueue) submitHigh(w work) {
	q.highPriority <- w
}

func (q *workQueue) start() {
	go q.run()
}

func (q *workQueue) run() {
	for {
		// read items off the high priority queue until it's empty
		select {
		case item := <-q.highPriority:
			item()
		default:
			select { // read one item from either queue, then go round the loop again
			case item := <-q.highPriority:
				item()
			case item := <-q.lowPriority:
				item()
			}
		}
	}
}
