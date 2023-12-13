package download

import "context"

// workQueue takes work items and executes them serially, in strict FIFO order
type workQueue struct {
	queue chan work
}

type work func(ctx context.Context)

func newWorkQueue(depth int) *workQueue {
	return &workQueue{queue: make(chan work, depth)}
}

func (q *workQueue) submit(w work) {
	q.queue <- w
}

func (q *workQueue) start() {
	go q.run()
}

func (q *workQueue) run() error {
	for item := range q.queue {
		item(context.Background())
	}
	return nil
}
