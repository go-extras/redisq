package redisq

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type FailureWorker struct {
	Worker
	MaxAttempts int
	SleepTime   int
}

// Instantiates FailureWorker class
// In addition it is possible to set exported parameters (Logger, MaxAttempts, SleepTime)
func NewFailureWorker(id string, conn redis.Conn, prefix, taskType string, handler WorkerHandler, failure chan error) (w *FailureWorker) {
	w = &FailureWorker{}

	w.rc = NewRedisClient(
		conn,
		prefix,
		taskType,
	)

	w.id = id
	w.handler = handler
	w.MaxAttempts = 5
	w.SleepTime = 10 //ms
	w.failure = failure
	w.Logger = &NullLogger{}

	return w
}

func (w *FailureWorker) processTask(uuid string) {
	w.Logger.Debugf("Processing previously failed task id: %s", uuid)

	// remove from the processing list on task finish
	defer func() {
		if err := w.rc.RemoveOneFromList(uuid, LIST_FAILURE_PROCESSING); err != nil {
			w.Logger.Errorf("RemoveOneFromList(\"%s\", \"%s\") call failed: %+v", uuid, LIST_FAILURE_PROCESSING, err)
		}
	}()

	// sleep so that we don't process this task immediately
	w.Logger.Debugf("Sleeping %dms", w.SleepTime)
	time.Sleep(time.Duration(w.SleepTime) * time.Millisecond)

	// obtain task details
	taskDetails, err := w.rc.GetTaskDetails(uuid)
	if err != nil {
		w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL)
		if err := w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL); err != nil {
			w.Logger.Errorf("PushTaskToList(\"%s\", \"%s\") call failed: %+v", uuid, LIST_FAILURE_FINAL, err)
		}
		return
	}

	// return the task back to the queue, if it yet has attempts to try
	if taskDetails.Attempts < w.MaxAttempts {
		w.rc.PushTaskToList(uuid, LIST_QUEUE)
		return
	}

	// run task handler
	err = w.handler(taskDetails.Arguments)

	// delete task if no error in handler
	if err == nil {
		w.Logger.Debug("Deleting task:", uuid)
		// delete a processed task, if success
		if err := w.rc.DeleteTask(uuid); err != nil {
			w.Logger.Errorf("DeleteTask(\"%s\") call failed: %+v", uuid, err)
		}
		return
	}

	// otherwise put the task to the failure queue
	w.Logger.Errorf("Handler call for task \"%s\" failed: %+v. ", uuid, err)
	if err := w.rc.PushTaskToList(uuid, LIST_FAILURE_FINAL); err != nil {
		w.Logger.Errorf("PushTaskToList(\"%s\", \"%s\") call failed: %+v", uuid, LIST_FAILURE_FINAL, err)
	}
}

// Get worker instance id
func (w *FailureWorker) GetInstanceId() string {
	return w.id
}

// Get worker instance id
func (w *FailureWorker) GetTaskType() string {
	return w.rc.taskType
}

// Run a worker (normally use a goroutine to allow concurent workers)
func (w *FailureWorker) Run() {
	for {
		// pick an item from the queue
		uuid, err := w.rc.PickTask(LIST_FAILURE, LIST_FAILURE_PROCESSING)

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
