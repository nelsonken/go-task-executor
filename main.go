package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"sync"
)

type Task struct {
	F              func(ctx context.Context) interface{}
	DependNames    []string
	Name           string
	DependsResults chan interface{}
	Children       []chan interface{}
}

var registry = map[string]*Task{}

func DoThings(tasks []*Task) error {

	var taskGraph = map[string]*Task{}

	for _, task := range tasks {
		if len(task.DependNames) > 0 {
			task.DependsResults = make(chan interface{}, len(task.DependNames)+1)
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
		go func(task *Task) {
			defer wg.Done()
			ctx := context.Background()
			deps := []interface{}{}
			for i := len(task.DependNames); i > 0; i-- {
				depData := <-task.DependsResults
				deps = append(deps, depData)
			}
			fmt.Printf("%s deps: %+v task done", task.Name, deps)
			data := task.F(ctx)
			for _, ch := range task.Children {
				ch <- data
			}
		}(task)
	}
	wg.Wait()

	return nil

}

func main() {
	var xs = []*Task{
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
			DependNames: []string{"c"},
			Name:        "b",
		},
		{
			F: func(ctx context.Context) interface{} {
				fmt.Println("c")
				return 3
			},
			DependNames: []string{},
			Name:        "c",
		},
		{
			F: func(ctx context.Context) interface{} {
				fmt.Println("d")
				return 4
			},
			DependNames: []string{},
			Name:        "d",
		},
	}

	DoThings(xs)
}
