package main

import (
	"context"
	"github.com/pkg/errors"
	"sync"
)

// Executor Concurrency / Normal TaskExecutor, can Auto Resolve Deps
type Executor struct {
	registry map[string]Task
}

func NewExecutor(registry map[string]Task) *Executor {
	return &Executor{registry: registry}
}

func (executor *Executor) ExecuteConcurrency(ctx context.Context, tasks []Task, results chan TaskResult) error {
	var taskGraph = map[string]Task{}

	for _, task := range tasks {
		taskGraph[task.Name()] = task
	}

	for _, task := range tasks {
		for _, name := range task.DepNames() {
			depsTask, ok := taskGraph[name]
			if ok {
				depsTask.AddChild(task.DepResultsChan())
			} else if depsTask, ok = executor.registry[name]; ok {
				depsTask.AddChild(task.DepResultsChan())
			} else {
				// you can get from other place
				return errors.New("dependency " + name + " not found")
			}
		}
	}

	wg := new(sync.WaitGroup)
	for _, task := range taskGraph {
		wg.Add(1)
		go func(task Task) {
			defer wg.Done()

			var deps []TaskResult

			for i := len(task.DepNames()); i > 0; i-- {
				depData := <-task.DepResultsChan()
				deps = append(deps, depData)
				ctx = context.WithValue(ctx, depData.GetName(), depData)
			}

			// deps done
			// fmt.Printf("%s deps: %+v task done\n", task.Name, deps)

			data, err := task.Do(ctx)
			r := &Result{
				Name: task.Name(),
				Data: data,
				Err:  err,
			}

			results <- r

			for _, ch := range task.Children() {
				ch <- r
			}
		}(task)
	}
	wg.Wait()
	close(results)

	return nil
}

func (executor *Executor) Execute(ctx context.Context, taskList []Task, results map[string]TaskResult) error {
	var tasks = map[string]Task{}

	for _, task := range taskList {
		tasks[task.Name()] = task
	}

	executor.doTask(tasks, ctx, results)

	return nil
}

func (executor *Executor) doTask(tasks map[string]Task, ctx context.Context, results map[string]TaskResult) {
	undoTasks := map[string]Task{}
	for _, task := range tasks {
		// well ! no deps
		if len(task.DepNames()) == 0 {
			i, err := task.Do(ctx)
			results[task.Name()] = &Result{
				Name: task.Name(),
				Data: i,
				Err:  err,
			}

		} else {
			var deps []TaskResult
			for _, name := range task.DepNames() {
				v, ok := results[name]
				if ok {
					deps = append(deps, v)
				}
			}

			// deps not enough!
			if len(deps) < len(task.DepNames()) {
				undoTasks[task.Name()] = task
			} else {
				if _, ok := undoTasks[task.Name()]; ok {
					delete(undoTasks, task.Name())
				}

				// good ! let's go
				for _, dep := range deps {
					ctx = context.WithValue(ctx, dep.GetName(), dep)
				}
				i, err := task.Do(ctx)
				results[task.Name()] = &Result{
					Name: task.Name(),
					Data: i,
					Err:  err,
				}
			}
		}
	}

	if len(tasks) > 0 {
		executor.doTask(undoTasks, ctx, results)
	}
}

type Task interface {
	Do(ctx context.Context) (interface{}, error)
	Name() string
	DepNames() []string
	DepResultsChan() chan TaskResult
	Children() []chan TaskResult
	AddChild(chan TaskResult)
}

type TaskResult interface {
	GetName() string
	GetData() interface{}
	GetError() error
}

type Result struct {
	Name string
	Data interface{}
	Err  error
}

func (r *Result) GetName() string {
	return r.Name
}

func (r *Result) GetData() interface{} {
	return r.Data
}

func (r *Result) GetError() error {
	return r.Err
}
