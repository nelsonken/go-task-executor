package executor

import (
	"context"
	"github.com/pkg/errors"
)

type TaskBase struct {
	depResultChan chan TaskResult
	children      []chan TaskResult
	input         interface{}
}

func (t *TaskBase) Name() string {
	return "not implements"
}

func (t *TaskBase) GetInput() interface{} {
	return t.input
}

func (t *TaskBase) SetInput(i interface{}) {
	t.input = i
}

func (t *TaskBase) Do(ctx context.Context) (interface{}, error) {
	return nil, errors.New("not implement")
}

func (t *TaskBase) DepNames() []string {
	return []string{}
}

func (t *TaskBase) DepResultsChan() chan TaskResult {
	if t.depResultChan == nil {
		t.depResultChan = make(chan TaskResult, len(t.DepNames()))
	}

	return t.depResultChan
}

func (t *TaskBase) Children() []chan TaskResult {
	return t.children
}

func (t *TaskBase) AddChild(results chan TaskResult) {
	if t.children == nil {
		t.children = []chan TaskResult{results}
		return
	}
	t.children = append(t.children, results)
}
