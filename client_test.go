package redisq

import (
	"encoding/json"
	"fmt"
	"github.com/rafaeljusto/redigomock"
	"reflect"
	"testing"
)

const (
	CLIENT_REDIS_PREFIX = "foo"
	CLIENT_TASK_TYPE    = "dummy"
	CLIENT_TASK_UUID    = "dummy_task_uuid_id"
)

func getRedisClient(conn *redigomock.Conn) *RedisClient {
	return NewRedisClient(conn, CLIENT_REDIS_PREFIX, CLIENT_TASK_TYPE)
}

func getClientTaskDetails() *TaskDetails {
	return &TaskDetails{
		Arguments:   []string{"foo", "bar", "next"},
		CreatedAt:   "Friday, 22-Nov-63 12:30:00 CST",
		Attempts:    0,
		Type:        CLIENT_TASK_TYPE,
		LastAttempt: "",
	}
}

func TestRedisClient_NewTask(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("BRPOPLPUSH",
		fmt.Sprintf("%s:%s:%s", CLIENT_REDIS_PREFIX, "from", CLIENT_TASK_TYPE),
		fmt.Sprintf("%s:%s:%s", CLIENT_REDIS_PREFIX, "to", CLIENT_TASK_TYPE),
		0,
	).Expect([]byte(CLIENT_TASK_UUID))

	client := getRedisClient(conn)
	uuid, err := client.PickTask("from", "to")

	if err != nil {
		t.Fatal(err)
	}

	if uuid != CLIENT_TASK_UUID {
		t.Errorf("Expected %+v got %+v", CLIENT_TASK_UUID, uuid)
		t.FailNow()
	}

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}

func TestRedisClient_GetTaskDetails(t *testing.T) {
	originalTaskDetails := getClientTaskDetails()
	jsonTaskDetails, err := json.Marshal(originalTaskDetails)

	if err != nil {
		t.Fatal(err)
	}

	conn := redigomock.NewConn()
	conn.Command("GET",
		fmt.Sprintf("%s:%s:%s:%s", CLIENT_REDIS_PREFIX, QUEUE_TASK, CLIENT_TASK_TYPE, CLIENT_TASK_UUID),
	).Expect([]byte(jsonTaskDetails))

	client := getRedisClient(conn)
	taskDetails, err := client.GetTaskDetails(CLIENT_TASK_UUID)

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(originalTaskDetails, taskDetails) {
		t.Error("Original task details and resulting task details do not match")
		t.FailNow()
	}

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}

func TestRedisClient_PushTaskToList(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("LPUSH",
		fmt.Sprintf("%s:%s:%s", CLIENT_REDIS_PREFIX, LIST_FAILURE_FINAL, CLIENT_TASK_TYPE),
		CLIENT_TASK_UUID,
	)

	client := getRedisClient(conn)
	err := client.PushTaskToList(CLIENT_TASK_UUID, LIST_FAILURE_FINAL)

	if err != nil {
		t.Fatal(err)
	}

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}

func TestRedisClient_SaveTaskDetails(t *testing.T) {
	originalTaskDetails := getClientTaskDetails()
	jsonTaskDetails, err := json.Marshal(originalTaskDetails)

	conn := redigomock.NewConn()
	conn.Command(
		"SET",
		fmt.Sprintf("%s:%s:%s:%s", CLIENT_REDIS_PREFIX, QUEUE_TASK, CLIENT_TASK_TYPE, CLIENT_TASK_UUID),
		jsonTaskDetails,
	)

	client := getRedisClient(conn)
	err = client.SaveTaskDetails(CLIENT_TASK_UUID, originalTaskDetails)

	if err != nil {
		t.Fatal(err)
	}

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}

func TestRedisClient_DeleteTask(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command(
		"DEL",
		fmt.Sprintf("%s:%s:%s:%s", CLIENT_REDIS_PREFIX, QUEUE_TASK, CLIENT_TASK_TYPE, CLIENT_TASK_UUID),
	)

	client := getRedisClient(conn)
	err := client.DeleteTask(CLIENT_TASK_UUID)

	if err != nil {
		t.Fatal(err)
	}

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}

func TestRedisClient_RemoveOneFromList(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("LREM",
		fmt.Sprintf("%s:%s:%s", CLIENT_REDIS_PREFIX, LIST_FAILURE_FINAL, CLIENT_TASK_TYPE),
		1,
		CLIENT_TASK_UUID,
	)

	client := getRedisClient(conn)
	err := client.RemoveOneFromList(CLIENT_TASK_UUID, LIST_FAILURE_FINAL)

	if err != nil {
		t.Fatal(err)
	}

	if len(conn.Errors) > 0 {
		t.Fatal(conn.Errors)
	}
}

func TestTaskDetails_NewAttempt(t *testing.T) {
	td := getClientTaskDetails()

	td.NewAttempt()

	if td.Attempts != 1 {
		t.Errorf("NewAttempt() is expected to increment attempts count by 1 (expected %d, got %d)", 1, td.Attempts)
		t.FailNow()
	}

	if td.LastAttempt == "" {
		t.Error("NewAttempt() is expected to set up LastAttempt date")
		t.FailNow()
	}
}
