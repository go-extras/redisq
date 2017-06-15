package redisq

import (
	"encoding/json"
	"fmt"
	"github.com/rafaeljusto/redigomock"
	"reflect"
	"testing"
)

const (
	WORKER_REDIS_PREFIX = "foo"
	WORKER_TASK_TYPE    = "dummy"
	WORKER_TASK_UUID    = "dummy_task_uuid_id"
)

func getWorkerTaskDetails() *TaskDetails {
	return &TaskDetails{
		Arguments:   []string{"foo", "bar", "next"},
		CreatedAt:   "Friday, 22-Nov-63 12:30:00 CST",
		Attempts:    0,
		Type:        CLIENT_TASK_TYPE,
		LastAttempt: "",
	}
}

func getRedisConnMock(t *testing.T) *redigomock.Conn {
	conn := redigomock.NewConn()
	// RemoveOneFromList
	conn.Command(
		"LREM",
		fmt.Sprintf("%s:%s:%s", WORKER_REDIS_PREFIX, LIST_PROCESSING, WORKER_TASK_TYPE),
		1,
		WORKER_TASK_UUID,
	)

	// GetTaskDetails
	originalTaskDetails := getWorkerTaskDetails()
	jsonTaskDetails, err := json.Marshal(originalTaskDetails)
	if err != nil {
		t.Fatal(err)
	}
	conn.Command(
		"GET",
		fmt.Sprintf("%s:%s:%s:%s", WORKER_REDIS_PREFIX, QUEUE_TASK, WORKER_TASK_TYPE, WORKER_TASK_UUID),
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
		fmt.Sprintf("%s:%s:%s:%s", WORKER_REDIS_PREFIX, QUEUE_TASK, WORKER_TASK_TYPE, WORKER_TASK_UUID),
		jsonModifiedTaskDetails,
	)

	// PushTaskToList
	conn.Command(
		"LPUSH",
		fmt.Sprintf("%s:%s:%s", WORKER_REDIS_PREFIX, LIST_FAILURE_FINAL, WORKER_TASK_TYPE),
		WORKER_TASK_UUID,
	)

	// PushTaskToList
	conn.Command(
		"LPUSH",
		fmt.Sprintf("%s:%s:%s", WORKER_REDIS_PREFIX, LIST_FAILURE_FINAL, WORKER_TASK_TYPE),
		WORKER_TASK_UUID,
	)

	// PushTaskToList
	conn.Command(
		"DEL",
		fmt.Sprintf("%s:%s:%s:%s", WORKER_REDIS_PREFIX, QUEUE_TASK, WORKER_TASK_TYPE, WORKER_TASK_UUID),
	)

	return conn
}

func TestWorker_processTask(t *testing.T) {
	failure := make(chan error, 0)
	conn := getRedisConnMock(t)

	handler := WorkerHandler(func(args []string) error {
		expectedArgs := []string{"foo", "bar", "next"}

		if !reflect.DeepEqual(args, expectedArgs) {
			t.Errorf("Task details do not match, expected %+v, got %+v", expectedArgs, args)
			t.FailNow()
		}
		return nil
	})

	w := NewWorker("worker 1", conn, WORKER_REDIS_PREFIX, WORKER_TASK_TYPE, handler, failure)

	w.processTask(WORKER_TASK_UUID)

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}

func TestWorker_GetInstanceId(t *testing.T) {
	failure := make(chan error, 0)
	conn := getFailureRedisConnMock(t)

	expected := "failure_worker_1"
	w := NewWorker(expected, conn, WORKER_REDIS_PREFIX, WORKER_TASK_TYPE, nil, failure)
	got := w.GetInstanceId()

	if got != expected {
		t.Errorf("Unexpected instance id value, expected %+v, got %+v", expected, got)
		t.FailNow()
	}
}
