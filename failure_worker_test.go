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

func TestFailureWorker_processTask(t *testing.T) {
	failure := make(chan error, 0)
	conn := getFailureRedisConnMock(t)

	handler := WorkerHandler(func(args []string) error {
		expectedArgs := []string{"foo", "bar", "next"}

		if !reflect.DeepEqual(args, expectedArgs) {
			t.Errorf("Task details do not match, expected %+v, got %+v", expectedArgs, args)
			t.FailNow()
		}
		return nil
	})

	w := NewFailureWorker("failure_worker_1", conn, FAILURE_WORKER_REDIS_PREFIX, FAILURE_WORKER_TASK_TYPE, handler, failure)
	w.processTask(FAILURE_WORKER_TASK_UUID)

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}

func TestFailureWorker_GetInstanceId(t *testing.T) {
	failure := make(chan error, 0)
	conn := getFailureRedisConnMock(t)

	expected := "failure_worker_1"
	w := NewFailureWorker(expected, conn, FAILURE_WORKER_REDIS_PREFIX, FAILURE_WORKER_TASK_TYPE, nil, failure)
	got := w.GetInstanceId()

	if got != expected {
		t.Errorf("Unexpected instance id value, expected %+v, got %+v", expected, got)
		t.FailNow()
	}
}

func TestFailureWorker_GetTaskType(t *testing.T) {
	failure := make(chan error, 0)
	conn := getFailureRedisConnMock(t)

	w := NewFailureWorker("failure_worker_1", conn, FAILURE_WORKER_REDIS_PREFIX, FAILURE_WORKER_TASK_TYPE, nil, failure)
	got := w.GetTaskType()

	if got != FAILURE_WORKER_TASK_TYPE {
		t.Errorf("Unexpected instance id value, expected %+v, got %+v", FAILURE_WORKER_TASK_TYPE, got)
		t.FailNow()
	}
}
