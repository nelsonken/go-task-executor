package main

import (
	"context"
	"fmt"
)

func main() {
	//	testDemo()

	taskList := []Task{
		&TaskA{},
		&TaskB{},
		&TaskC{},
		&TaskD{},
	}
	var err error
	ctx := context.Background()
	executor := NewExecutor(map[string]Task{})

	results := map[string]TaskResult{}
	err = executor.Execute(ctx, taskList, results)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range results {
		fmt.Printf("%+v\n", v)
	}
	fmt.Println("---------------------------")
	resultChan := make(chan TaskResult, len(taskList))
	err = executor.ExecuteConcurrency(ctx, taskList, resultChan)
	if err != nil {
		fmt.Println(err)
	}
	for x := range resultChan {
		fmt.Printf("%+v\n", x)
	}
}

type TaskA struct {
	TaskBase
}

func (t *TaskA) Name() string {
	return "a"
}

func (t *TaskA) DepNames() []string {
	return []string{"b", "c", "d"}
}

func (t *TaskA) Do(ctx context.Context) (interface{}, error) {
	return "a done", nil
}

func (t *TaskA) DepResultsChan() chan TaskResult {
	if t.depResultChan == nil {
		t.depResultChan = make(chan TaskResult, len(t.DepNames()))
	}

	return t.depResultChan
}

type TaskB struct {
	TaskBase
}

func (t *TaskB) Name() string {
	return "b"
}

func (t *TaskB) DepNames() []string {
	return []string{"c", "d"}
}

func (t *TaskB) Do(ctx context.Context) (interface{}, error) {
	return "b done", nil
}

func (t *TaskB) DepResultsChan() chan TaskResult {
	if t.depResultChan == nil {
		t.depResultChan = make(chan TaskResult, len(t.DepNames()))
	}

	return t.depResultChan
}

type TaskC struct {
	TaskBase
}

func (t *TaskC) Name() string {
	return "c"
}

func (t *TaskC) DepNames() []string {
	return []string{}
}

func (t *TaskC) Do(ctx context.Context) (interface{}, error) {
	return "c done", nil
}

func (t *TaskC) DepResultsChan() chan TaskResult {
	if t.depResultChan == nil {
		t.depResultChan = make(chan TaskResult, len(t.DepNames()))
	}

	return t.depResultChan
}

type TaskD struct {
	TaskBase
}

func (t *TaskD) Name() string {
	return "d"
}

func (t *TaskD) DepNames() []string {
	return []string{"c"}
}

func (t *TaskD) Do(ctx context.Context) (interface{}, error) {
	return "d done", nil
}

func (t *TaskD) DepResultsChan() chan TaskResult {
	if t.depResultChan == nil {
		t.depResultChan = make(chan TaskResult, len(t.DepNames()))
	}

	return t.depResultChan
}
