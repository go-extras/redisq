package redisq

import "fmt"

type WorkerError struct {
	Worker WorkerInterface
	Err    error
}

func (w WorkerError) Error() string {
	return fmt.Sprintf("Worker Id \"%s\" failed with error \"%+v\"", w.Worker.GetInstanceId(), w.Err)
}

type WorkerFatalError struct {
	WorkerError
}

func (w WorkerFatalError) Error() string {
	return fmt.Sprintf("Worker Id \"%s\" failed with error \"%+v\"", w.Worker.GetInstanceId(), w.Err)
}
