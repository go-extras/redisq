package redisq

import (
	"github.com/garyburd/redigo/redis"
)

type WorkerInterface interface {
	GetInstanceId() string
	GetTaskType() string
	Run()
}

// Defines Worker handler function
type WorkerHandler (func(args []string) error)

type Worker struct {
	WorkerInterface
	id      string
	rc      *RedisClient
	failure chan error
	handler WorkerHandler
	Logger  Logger
}

// Instantiates Worker class
// In addition it is possible to set exported parameters (Logger)
func NewWorker(id string, conn redis.Conn, prefix, taskType string, handler WorkerHandler, failure chan error) (w *Worker) {
	w = &Worker{
		id:      id,
		handler: handler,
		rc: NewRedisClient(
			conn,
			prefix,
			taskType,
		),
		failure: failure,
		Logger:  &NullLogger{},
	}

	return w
}

func (w *Worker) processTask(uuid string) {
	w.Logger.Debugf("Processing task id: %s", uuid)

	// remove from the processing list on task finish
	defer func() {
		w.Logger.Debugf("Removing %s from %s", uuid, LIST_PROCESSING)
		if err := w.rc.RemoveOneFromList(uuid, LIST_PROCESSING); err != nil {
			w.Logger.Errorf("RemoveOneFromList(\"%s\", \"%s\") call failed: %+v", uuid, LIST_PROCESSING, err)
		}
	}()

	// obtain task details
	w.Logger.Debugf("Getting %s details", uuid)
	taskDetails, err := w.rc.GetTaskDetails(uuid)
	if err != nil {
		w.Logger.Errorf("GetTaskDetails(\"%s\") call failed: %+v", uuid, err)
		w.Logger.Debugf("Pushing %s to %s", uuid, LIST_FAILURE_FINAL)
		if err := w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL); err != nil {
			w.Logger.Errorf("PushTaskToList(\"%s\", \"%s\") call failed: %+v", uuid, LIST_FAILURE_FINAL, err)
		}
		return
	}

	// Increment task attempt counter
	taskDetails.NewAttempt()

	// Try to save updated task state
	w.Logger.Debugf("Saving %s details (new attempts count: %d)", uuid, taskDetails.Attempts)
	err = w.rc.SaveTaskDetails(uuid, taskDetails)
	if err != nil {
		w.Logger.Errorf("SaveTaskDetails(\"%s\") call failed: %+v", uuid, err)
		w.Logger.Debugf("Pushing %s to %s", uuid, LIST_FAILURE_FINAL)
		if err := w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL); err != nil {
			w.Logger.Errorf("PushTaskToList(\"%s\", \"%s\") call failed: %+v", uuid, LIST_FAILURE_FINAL, err)
		}
		return
	}

	// handle task
	w.Logger.Debugf("Calling %s handler with args %+v", uuid, taskDetails.Arguments)
	err = w.handler(taskDetails.Arguments)

	if err == nil {
		w.Logger.Debug("Deleting task:", uuid)
		// delete a processed task, if success
		if err := w.rc.DeleteTask(uuid); err != nil {
			w.Logger.Errorf("DeleteTask(\"%s\") call failed: %+v", uuid, err)
		}
	} else {
		// otherwise put the task to the failure queue
		w.Logger.Errorf("Handler call for task \"%s\" failed: %+v", uuid, err)
		w.Logger.Debugf("Pushing %s to %s", uuid, LIST_FAILURE)
		if err := w.rc.PushTaskToList(uuid, LIST_FAILURE); err != nil {
			w.Logger.Errorf("PushTaskToList(\"%s\", \"%s\") call failed: %+v", uuid, LIST_FAILURE, err)
		}
	}
}

// Get worker instance id
func (w *Worker) GetInstanceId() string {
	return w.id
}

// Get worker instance id
func (w *Worker) GetTaskType() string {
	return w.rc.taskType
}

// Run a worker (normally use a goroutine to allow concurent workers)
func (w *Worker) Run() {
	w.Logger.Debugf("[%s][%s] started", w.GetInstanceId(), w.GetTaskType())
	for {
		// pick an item from the queue
		uuid, err := w.rc.PickTask(LIST_QUEUE, LIST_PROCESSING)

		if err != nil {
			w.failure <- WorkerFatalError{
				WorkerError: WorkerError{
					Worker: w,
					Err:    err,
				},
			}
			return
		}

		w.processTask(uuid)
	}
}
