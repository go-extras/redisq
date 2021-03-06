package redisq

import (
	"time"

	"fmt"
	"github.com/garyburd/redigo/redis"
)

type FailureWorker struct {
	Worker
	MaxAttempts int
	SleepTime   int
}

// Instantiates FailureWorker class
// In addition it is possible to set exported parameters (Logger, MaxAttempts, SleepTime)
func NewFailureWorker(id int, conn redis.Conn, prefix, taskType string, handler WorkerHandler, failure chan error) (w *FailureWorker) {
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

func (w *FailureWorker) markTaskAsFailed(uuid string, err error, taskDetails *TaskDetails, permanently bool) error {
	var list = LIST_FAILURE

	if permanently {
		list = LIST_FAILURE_FINAL
	}

	if taskDetails != nil {
		taskDetails.LastError = fmt.Sprintf("%+v", err)
		w.rc.SaveTaskDetails(uuid, taskDetails)
	}

	w.Logger.Debugf("Pushing %s to %s", uuid, list)
	if err := w.rc.PushTaskToList(uuid, list); err != nil {
		w.Logger.Errorf("PushTaskToList(\"%s\", \"%s\") call failed: %+v", uuid, list, err)
		return err
	}

	return nil
}

func (w *FailureWorker) processTask(uuid string) {
	w.Logger.Debugf("Processing previously failed task id: %s", uuid)

	// remove from the processing list on task finish
	defer func() {
		w.Logger.Debugf("Removing %s from %s", uuid, LIST_FAILURE_PROCESSING)
		if err := w.rc.RemoveOneFromList(uuid, LIST_FAILURE_PROCESSING); err != nil {
			w.Logger.Errorf("RemoveOneFromList(\"%s\", \"%s\") call failed: %+v", uuid, LIST_FAILURE_PROCESSING, err)
		}
	}()

	// sleep so that we don't process this task immediately
	w.Logger.Debugf("Sleeping %dms", w.SleepTime)
	time.Sleep(time.Duration(w.SleepTime) * time.Millisecond)

	// obtain task details
	w.Logger.Debugf("Getting %s details", uuid)
	taskDetails, err := w.rc.GetTaskDetails(uuid)
	if err != nil {
		w.markTaskAsFailed(uuid, err, nil, true)
		return
	}

	// return the task back to the queue, if it yet has attempts to try
	if taskDetails.Attempts < w.MaxAttempts {
		w.Logger.Debugf("Pushing %s to %s", uuid, LIST_QUEUE)
		w.rc.PushTaskToList(uuid, LIST_QUEUE)
		return
	}

	// run task handler
	w.Logger.Debugf("Calling %s failure handler with args %+v", uuid, taskDetails.Arguments)
	err = w.handler(w.Logger, taskDetails.Arguments)

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
	w.markTaskAsFailed(uuid, err, taskDetails, true)
}

// Get worker instance id
func (w *FailureWorker) GetInstanceId() int {
	return w.id
}

// Get worker instance id
func (w *FailureWorker) GetTaskType() string {
	return w.rc.taskType
}

// Run a worker (normally use a goroutine to allow concurrent workers)
func (w *FailureWorker) Run() {
	w.Logger.Debug("started")
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
