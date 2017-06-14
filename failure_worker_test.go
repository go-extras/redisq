package redisq

import (
	"encoding/json"
	"fmt"
	"github.com/rafaeljusto/redigomock"
	"reflect"
	"testing"
)

const (
	FAILURE_WORKER_REDIS_PREFIX = "foo"
	FAILURE_WORKER_TASK_TYPE    = "dummy"
	FAILURE_WORKER_TASK_UUID    = "dummy_task_uuid_id"
)

func getFailureRedisConnMock(t *testing.T) *redigomock.Conn {
	conn := redigomock.NewConn()
	// RemoveOneFromList
	conn.Command(
		"LREM",
		fmt.Sprintf("%s:%s:%s", FAILURE_WORKER_REDIS_PREFIX, LIST_PROCESSING, FAILURE_WORKER_TASK_TYPE),
		1,
		FAILURE_WORKER_TASK_UUID,
	)

	// GetTaskDetails
	originalTaskDetails := getWorkerTaskDetails()
	jsonTaskDetails, err := json.Marshal(originalTaskDetails)
	if err != nil {
		t.Fatal(err)
	}
	conn.Command(
		"GET",
		fmt.Sprintf("%s:%s:%s:%s", FAILURE_WORKER_REDIS_PREFIX, QUEUE_TASK, FAILURE_WORKER_TASK_TYPE, FAILURE_WORKER_TASK_UUID),
	).Expect([]byte(jsonTaskDetails))

	// SaveTaskDetails
	modifiedTaskDetails := originalTaskDetails
	modifiedTaskDetails.NewAttempt()
	jsonModifiedTaskDetails, err := json.Marshal(modifiedTaskDetails)
	if err != nil {
		t.Fatal(err)
	}
	conn.Command(
		"SET",
		fmt.Sprintf("%s:%s:%s:%s", FAILURE_WORKER_REDIS_PREFIX, QUEUE_TASK, FAILURE_WORKER_TASK_TYPE, FAILURE_WORKER_TASK_UUID),
		jsonModifiedTaskDetails,
	)

	// PushTaskToList
	conn.Command(
		"LPUSH",
		fmt.Sprintf("%s:%s:%s", FAILURE_WORKER_REDIS_PREFIX, LIST_FAILURE_FINAL, FAILURE_WORKER_TASK_TYPE),
		FAILURE_WORKER_TASK_UUID,
	)

	// PushTaskToList
	conn.Command(
		"LPUSH",
		fmt.Sprintf("%s:%s:%s", FAILURE_WORKER_REDIS_PREFIX, LIST_QUEUE, FAILURE_WORKER_TASK_TYPE),
		FAILURE_WORKER_TASK_UUID,
	)

	// PushTaskToList
	conn.Command(
		"DEL",
		fmt.Sprintf("%s:%s:%s:%s", FAILURE_WORKER_REDIS_PREFIX, QUEUE_TASK, FAILURE_WORKER_TASK_TYPE, FAILURE_WORKER_TASK_UUID),
	)

	return conn
}

func TestFailureWorker_ProcessTask(t *testing.T) {
	quit := make(chan bool, 0)
	conn := getFailureRedisConnMock(t)

	handler := WorkerHandler(func(args []string) error {
		expectedArgs := []string{"foo", "bar", "next"}

		if !reflect.DeepEqual(args, expectedArgs) {
			t.Errorf("Task details do not match, expected %+v, got %+v", expectedArgs, args)
			t.FailNow()
		}
		return nil
	})

	w := NewFailureWorker(conn, FAILURE_WORKER_REDIS_PREFIX, FAILURE_WORKER_TASK_TYPE, handler, quit)
	w.ProcessTask(FAILURE_WORKER_TASK_UUID)

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}
