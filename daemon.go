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

func (d *Daemon) sleep(from, to int32) {
	n := rand.Int31n(to-from) + from // [5..15)
	d.Logger.Infof(" Sleeping %d seconds.", n)
	runtime.Gosched()
	time.Sleep(time.Duration(n) * time.Second)
}

func (d *Daemon) getRedisConn(addr string) redis.Conn {
	for {
		conn, err := redis.Dial("tcp", addr)

		if err != nil {
			d.Logger.Errorf("Cannot connect to Redis: %+v.", err)
			d.sleep(5, 15)
			continue
		}

		return conn
	}
}

func (d *Daemon) runWorker(id int) {
	conn := d.getRedisConn(d.redisAddr)
	worker := NewWorker(
		id,
		conn,
		d.redisPrefix,
		d.taskType,
		d.WorkerHandler,
		d.failureW,
	)
	worker.Logger = WrapLogger(d.Logger, fmt.Sprintf("[%s][%s][%d]", "w", d.taskType, id))
	go func(conn redis.Conn) {
		defer conn.Close()
		worker.Run()
	}(conn)
}

func (d *Daemon) runFailureWorker(id int) WorkerInterface {
	conn := d.getRedisConn(d.redisAddr)
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
					d.sleep(5, 15)
					d.runWorker(val.Worker.GetInstanceId())
				}()
			} else {
				d.Logger.Error(err)
			}
		case err := <-d.failureFW:
			if val, ok := err.(WorkerFatalError); ok {
				d.Logger.Errorf("[%s][%s] failed with error: %+v", val.Worker.GetInstanceId(), val.Worker.GetTaskType(), val.Err)
				go func() {
					d.sleep(5, 15)
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
		go d.runWorker(i)
	}

	go d.runFailureWorker(0)

	// restart workers on failure
	go d.workerErrorHandler()
}

// this is the only way how you should init the daemon (no direct instantiation)
func NewDaemon(taskType string, workerCount int, redisPrefix, redisAddr string) *Daemon {
	logger := &NullLogger{}

	workerHandler := WorkerHandler(func(logger Logger, args []string) error {
		logger.Printf("Task args: %s", strings.Join(args, " "))

		return nil
	})

	failureWorkerHandler := WorkerHandler(func(logger Logger, args []string) error {
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
