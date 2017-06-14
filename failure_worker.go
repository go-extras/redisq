package redisq

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

type FailureWorker struct {
	Worker
	MaxAttempts int
	SleepTime   int
}

func NewFailureWorker(conn redis.Conn, prefix, taskType string, handler WorkerHandler, quit chan bool) (w *FailureWorker) {
	w = &FailureWorker{}

	w.quit = quit
	w.rc = NewRedisClient(
		conn,
		prefix,
		taskType,
	)

	w.handler = handler
	w.MaxAttempts = 5
	w.SleepTime = 10 //ms

	return w
}

func (w *FailureWorker) ProcessTask(uuid string) {
	log.Print("Processing previously failed task UUID id:" + uuid)
	defer w.rc.RemoveOneFromList(uuid, LIST_FAILURE_PROCESSING)

	// sleep so that we don't process this task immediately
	time.Sleep(time.Duration(w.SleepTime) * time.Millisecond)

	taskDetails, err := w.rc.GetTaskDetails(uuid)
	if err != nil {
		log.Print(err)
		w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL)
		return
	}

	if taskDetails.Attempts < w.MaxAttempts {
		w.rc.PushTaskToList(uuid, LIST_QUEUE)
	} else {
		w.handler(taskDetails.Arguments)

		if err == nil {
			w.rc.DeleteTask(uuid)
		} else {
			w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL)
		}
	}
}

func (w *FailureWorker) Run() {
	for {
		// pick an item from the queue
		uuid, err := w.rc.PickTask(LIST_FAILURE, LIST_FAILURE_PROCESSING)

		if err != nil {
			log.Print(err)
			w.quit <- true
			return
		}

		w.ProcessTask(uuid)
	}
}
