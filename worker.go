package redisq

import (
	"log"

	"github.com/garyburd/redigo/redis"
)

type WorkerInterface interface {
	ProcessTask(string)
	Run()
}
type WorkerHandler (func(args []string) error)

type Worker struct {
	WorkerInterface
	rc      *RedisClient
	quit    chan bool
	handler WorkerHandler
}

func NewWorker(conn redis.Conn, prefix, taskType string, handler WorkerHandler, quit chan bool) (w *Worker) {
	w = &Worker{
		handler: handler,
		quit:    quit,
		rc: NewRedisClient(
			conn,
			prefix,
			taskType,
		),
	}

	return w
}

func (w *Worker) ProcessTask(uuid string) {
	log.Print("TaskTypeConfig UUID id:" + uuid)
	defer w.rc.RemoveOneFromList(uuid, LIST_PROCESSING)

	// obtain task details
	taskDetails, err := w.rc.GetTaskDetails(uuid)
	if err != nil {
		log.Print(err)
		w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL)
		return
	}

	// Increment task attempt counter
	taskDetails.NewAttempt()
	log.Printf("Task %s attempt count is %d", uuid, taskDetails.Attempts)

	// Try to save updated task state
	err = w.rc.SaveTaskDetails(uuid, taskDetails)
	if err != nil {
		log.Print(err)
		w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL)
		return
	}

	// handle task
	err = w.handler(taskDetails.Arguments)

	if err == nil {
		// delete a processed task, if success
		w.rc.DeleteTask(uuid)
	} else {
		// otherwise put the task to the failure queue
		log.Print(err)
		w.rc.PushTaskToList(uuid, LIST_FAILURE)
	}
}

func (w *Worker) Run() {
	for {
		// pick an item from the queue
		uuid, err := w.rc.PickTask(LIST_QUEUE, LIST_PROCESSING)

		if err != nil {
			log.Print(err)
			w.quit <- true
			return
		}

		w.ProcessTask(uuid)
	}
}
