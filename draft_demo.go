package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"sync"
)


type TaskDemo struct {
	F              func(ctx context.Context) interface{}
	DependNames    []string
	Name           string
	DependsResults chan interface{}
	Children       []chan interface{}
}

var registry = map[string]*TaskDemo{}

func ExecuteConcurrency1(tasks []*TaskDemo) error {
	var taskGraph = map[string]*TaskDemo{}

	for _, task := range tasks {
		if len(task.DependNames) > 0 {
			task.DependsResults = make(chan interface{}, len(task.DependNames))
		}

		taskGraph[task.Name] = task
	}

	for _, task := range tasks {
		for _, name := range task.DependNames {
			depsTask, ok := taskGraph[name]
			if ok {
				if depsTask.Children == nil {
					depsTask.Children = []chan interface{}{}
				}

				depsTask.Children = append(depsTask.Children, task.DependsResults)
			} else if depsTask, ok = registry[name]; ok {
				if depsTask.Children == nil {
					depsTask.Children = []chan interface{}{}
				}

				depsTask.Children = append(depsTask.Children, task.DependsResults)
			} else {
				// you can get from other place
				return errors.New("dependency " + name + " not found")
			}
		}
	}

	wg := new(sync.WaitGroup)
	for _, task := range taskGraph {
		wg.Add(1)
		go func(task *TaskDemo) {
			defer wg.Done()
			ctx := context.Background()
			deps := []interface{}{}
			for i := len(task.DependNames); i > 0; i-- {
				depData := <-task.DependsResults
				deps = append(deps, depData)
			}
			fmt.Printf("%s deps: %+v task done\n", task.Name, deps)
			data := task.F(ctx)
			for _, ch := range task.Children {
				ch <- data
			}
		}(task)
	}
	wg.Wait()

	return nil
}

func testDemo() {
	var tasks = []*TaskDemo{
		{
			F: func(ctx context.Context) interface{} {
				return 1
			},
			DependNames: []string{"b", "d", "c"},
			Name:        "a",
		},
		{
			F: func(ctx context.Context) interface{} {
				return 2
			},
			DependNames: []string{"c", "d"},
			Name:        "b",
		},
		{
			F: func(ctx context.Context) interface{} {
				return 3
			},
			DependNames: []string{},
			Name:        "c",
		},
		{
			F: func(ctx context.Context) interface{} {
				return 4
			},
			DependNames: []string{"c"},
			Name:        "d",
		},
	}

	ExecuteConcurrency1(tasks)
	fmt.Println("---------------")
	Execute1(tasks)
}

func Execute1(tasks []*TaskDemo) error {

	var taskGraph = map[string]*TaskDemo{}

	for _, task := range tasks {
		taskGraph[task.Name] = task
	}

	var results = map[string]interface{}{}

	ctx := context.Background()

	doTask(taskGraph, ctx, results)

	fmt.Printf("%+v", results)

	return nil
}

func doTask(taskGraph map[string]*TaskDemo, ctx context.Context, results map[string]interface{}) {
	undoTasks := map[string]*TaskDemo{}
	for _, task := range taskGraph {
		if len(task.DependNames) == 0 {
			i := task.F(ctx)
			results[task.Name] = i
		} else {
			deps := []interface{}{}
			for _, name := range task.DependNames {
				v, ok := results[name]
				if ok {
					deps = append(deps, v)
				}
			}

			if len(deps) < len(task.DependNames) {
				undoTasks[task.Name] = task
			} else {
				if _, ok := undoTasks[task.Name]; ok {
					delete(undoTasks, task.Name)
				}
				results[task.Name] = task.F(ctx)
			}
		}
	}

	if len(taskGraph) > 0 {
		doTask(undoTasks, ctx, results)
	}
}
