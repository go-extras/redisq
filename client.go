package redisq

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	QUEUE_TASK              = "task"
	LIST_QUEUE              = "queue"
	LIST_FAILURE            = "failure"
	LIST_FAILURE_FINAL      = "failure_final"
	LIST_PROCESSING         = "processing"
	LIST_FAILURE_PROCESSING = "failure_processing"
)

type TaskDetails struct {
	Arguments   []string `json:"arguments"`
	CreatedAt   string   `json:"createdAt"`
	Attempts    int      `json:"attempts"`
	Type        string   `json:"type"`
	LastAttempt string   `json:"lastAttempt"`
	LastError   string   `json:"lastError"`
}

// increments attempts and updates `LastAttempt` property to the current date
func (td *TaskDetails) NewAttempt() {
	td.Attempts++
	td.LastAttempt = time.Now().Format(time.RFC3339)
}

type RedisClient struct {
	conn     redis.Conn
	prefix   string
	taskType string
}

func NewRedisClient(conn redis.Conn, prefix, taskType string) *RedisClient {
	return &RedisClient{
		conn:     conn,
		prefix:   prefix,
		taskType: taskType,
	}
}

// pick an item from the queue
func (rc *RedisClient) PickTask(from, to string) (string, error) {
	result, err := rc.conn.Do(
		"BRPOPLPUSH",
		fmt.Sprintf("%s:%s:%s", rc.prefix, from, rc.taskType),
		fmt.Sprintf("%s:%s:%s", rc.prefix, to, rc.taskType),
		0,
	)

	if err != nil {
		return "", err
	}

	// interpret result as []byte
	uuid, ok := result.([]byte)
	if !ok {
		return "", errors.New("Interpreting task uuid interface{} as []byte failed")
	}

	return string(uuid), nil
}

// get task details for a given task uuid
func (rc *RedisClient) GetTaskDetails(uuid string) (*TaskDetails, error) {
	taskResult, err := rc.conn.Do("GET", fmt.Sprintf("%s:%s:%s:%s", rc.prefix, QUEUE_TASK, rc.taskType, uuid))
	if err != nil {
		return nil, err
	}

	// interpret result as []byte
	taskJson, ok := taskResult.([]byte)
	if !ok {
		return nil, errors.New("Interpreting task data interface{} as []byte failed")
	}

	var taskDetails TaskDetails
	err = json.Unmarshal(taskJson, &taskDetails)
	if err != nil {
		return nil, err
	}

	return &taskDetails, nil
}

func (rc *RedisClient) PushTaskToList(uuid string, list string) error {
	_, err := rc.conn.Do("LPUSH", fmt.Sprintf("%s:%s:%s", rc.prefix, list, rc.taskType), uuid)

	return err
}

func (rc *RedisClient) SaveTaskDetails(uuid string, taskDetails *TaskDetails) error {
	newResult, err := json.Marshal(taskDetails)

	if err == nil {
		_, err = rc.conn.Do("SET", fmt.Sprintf("%s:%s:%s:%s", rc.prefix, QUEUE_TASK, rc.taskType, uuid), newResult)
	}

	return err
}

func (rc *RedisClient) DeleteTask(uuid string) error {
	_, err := rc.conn.Do("DEL", fmt.Sprintf("%s:%s:%s:%s", rc.prefix, QUEUE_TASK, rc.taskType, uuid))

	return err
}

func (rc *RedisClient) RemoveOneFromList(uuid, listName string) error {
	_, err := rc.conn.Do("LREM", fmt.Sprintf("%s:%s:%s", rc.prefix, listName, rc.taskType), 1, uuid)

	return err
}
