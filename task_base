package main

import "context"

type TaskBase struct {
	depResultChan chan TaskResult
	children      []chan TaskResult
}

func (t *TaskBase) Do(ctx context.Context) (interface{}, error) {
	return " done", nil
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
