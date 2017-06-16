package redisq

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"runtime"
	"strings"
	"time"
)

type Daemon struct {
	redisPrefix          string
	redisAddr            string
	failureW             chan error
	failureFW            chan error
	taskType             string
	workerCount          int
	FailureMaxAttempts   int
	FailureSleepTime     int
	WorkerHandler        WorkerHandler
	FailureWorkerHandler WorkerHandler
	Logger               Logger
}

func (d *Daemon) sleep(id string, from, to int32) {
	n := rand.Int31n(to-from) + from // [5..15)
	d.Logger.Infof("[%s][%s] Sleeping %d seconds.", id, d.taskType, n)
	runtime.Gosched()
	time.Sleep(time.Duration(n) * time.Second)
}

func (d *Daemon) getRedisConn(id, taskType, addr string) redis.Conn {
	for {
		conn, err := redis.Dial("tcp", addr)

		if err != nil {
			d.Logger.Errorf("[%s][%s] Cannot connect to Redis: %+v.", id, taskType, err)
			d.sleep(id, 5, 15)
			continue
		}

		return conn
	}
}

func (d *Daemon) runWorker(id string) {
	conn := d.getRedisConn(id, d.taskType, d.redisAddr)
	worker := NewWorker(
		id,
		conn,
		d.redisPrefix,
		d.taskType,
		d.WorkerHandler,
		d.failureW,
	)
	worker.Logger = d.Logger
	go func(conn redis.Conn) {
		defer conn.Close()
		worker.Run()
	}(conn)
}

func (d *Daemon) runFailureWorker(id string) WorkerInterface {
	conn := d.getRedisConn(id, d.taskType, d.redisAddr)
	failureWorker := NewFailureWorker(
		id,
		conn,
		d.redisPrefix,
		d.taskType,
		d.FailureWorkerHandler,
		d.failureFW,
	)
	failureWorker.MaxAttempts = d.FailureMaxAttempts
	failureWorker.SleepTime = d.FailureSleepTime
	failureWorker.Logger = d.Logger
	go func(conn redis.Conn) {
		defer conn.Close()
		failureWorker.Run()
	}(conn)

	return failureWorker
}

func (d *Daemon) workerErrorHandler() {
	for {
		select {
		case err := <-d.failureW:
			if val, ok := err.(WorkerFatalError); ok {
				d.Logger.Errorf("[%s][%s] failed with error: %+v", val.Worker.GetInstanceId(), val.Worker.GetTaskType(), val.Err)
				go func() {
					d.sleep(val.Worker.GetInstanceId(), 5, 15)
					d.runWorker(val.Worker.GetInstanceId())
				}()
			} else {
				d.Logger.Error(err)
			}
		case err := <-d.failureFW:
			if val, ok := err.(WorkerFatalError); ok {
				d.Logger.Errorf("[%s][%s] failed with error: %+v", val.Worker.GetInstanceId(), val.Worker.GetTaskType(), val.Err)
				go func() {
					d.sleep(val.Worker.GetInstanceId(), 5, 15)
					d.runFailureWorker(val.Worker.GetInstanceId())
				}()
			} else {
				d.Logger.Error(err)
			}
		}
	}
}

// use this method to start the workers
func (d *Daemon) Run() {
	// initial start
	for i := 0; i < d.workerCount; i++ {
		go d.runWorker(fmt.Sprintf("Worker[%d]", i))
	}

	go d.runFailureWorker("FailureWorker")

	// restart workers on failure
	go d.workerErrorHandler()
}

// this is the only way how you should init the daemon (no direct instantiation)
func NewDaemon(taskType string, workerCount int, redisPrefix, redisAddr string) *Daemon {
	logger := &NullLogger{}

	workerHandler := WorkerHandler(func(args []string) error {
		logger.Printf("Task args: %s", strings.Join(args, " "))

		return nil
	})

	failureWorkerHandler := WorkerHandler(func(args []string) error {
		logger.Print("Failure task args: " + strings.Join(args, " "))

		return errors.New("Failure worker is not supported at the moment")
	})

	return &Daemon{
		redisPrefix:          redisPrefix,
		redisAddr:            redisAddr,
		failureW:             make(chan error, 0),
		failureFW:            make(chan error, 0),
		taskType:             taskType,
		workerCount:          workerCount,
		FailureMaxAttempts:   2,
		FailureSleepTime:     10000,
		WorkerHandler:        workerHandler,
		FailureWorkerHandler: failureWorkerHandler,
		Logger:               logger,
	}
}
