package executor

import (
	"context"
	"github.com/pkg/errors"
	"sync"
)

type jobStruct struct {
	t          Task
	ch         chan TaskResult
	ctx        context.Context
	needResult bool
}

// Executor Concurrency / Normal TaskExecutor, can Auto Resolve Deps
type Executor struct {
	registry    map[string]Task
	jobCh       chan jobStruct
	poolStarted bool
	poolSize    int
	locker      *sync.Mutex
}

func NewExecutor(registry map[string]Task, poolSize int) *Executor {
	jobCh := make(chan jobStruct, poolSize)

	return &Executor{registry: registry, jobCh: jobCh, poolSize: poolSize, locker: new(sync.Mutex)}
}

func (ex *Executor) StartPool() {
	ex.locker.Lock()
	defer ex.locker.Unlock()
	if ex.poolStarted {
		return
	}

	for i := ex.poolSize; i > 0; i-- {
		go func() {
			for job := range ex.jobCh {
				var deps []TaskResult

				for i := len(job.t.DepNames()); i > 0; i-- {
					depData := <-job.t.DepResultsChan()
					deps = append(deps, depData)
					job.ctx = context.WithValue(job.ctx, depData.GetName()+"_ret", depData)
				}

				data, err := job.t.Do(job.ctx)
				r := &Result{
					Name: job.t.Name(),
					Data: data,
					Err:  err,
				}

				if job.needResult {
					job.ch <- r
				}

				for _, ch := range job.t.Children() {
					ch <- r
				}

			}
		}()
	}

	ex.poolStarted = true
}

func (ex *Executor) StopPool() {
	ex.locker.Lock()
	defer ex.locker.Unlock()
	close(ex.jobCh)
}

func (ex *Executor) ExecuteConcurrencyWithPool(ctx context.Context, tasks []Task, results chan TaskResult) error {
	if !ex.poolStarted {
		return errors.New("pool not started")
	}

	taskGraph, noNeedResult, err := ex.resolveTaskDeps(tasks)
	if err != nil {
		return err
	}

	for _, task := range taskGraph {
		_, noNeedRes := noNeedResult[task.Name()]
		ex.jobCh <- jobStruct{
			t:          task,
			ch:         results,
			ctx:        ctx,
			needResult: !noNeedRes,
		}
	}

	return nil
}

func (ex *Executor) resolveTaskDeps(tasks []Task) (map[string]Task, map[string]struct{}, error) {
	var taskGraph = map[string]Task{}
	var noNeedResult = map[string]struct{}{}
	for _, task := range tasks {
		taskGraph[task.Name()] = task
	}

	for _, task := range tasks {
		for _, name := range task.DepNames() {
			depsTask, ok := taskGraph[name]
			if ok {
				depsTask.AddChild(task.DepResultsChan())
			} else if depsTask, ok = ex.registry[name]; ok {
				noNeedResult[depsTask.Name()] = struct{}{}
				taskGraph[name] = depsTask
				if depsTask.GetInput() == nil {
					depsTask.SetInput(task.GetInput())
				}

				depsTask.AddChild(task.DepResultsChan())
			} else {
				// you can get from other place
				return nil, nil, errors.New("dependency " + name + " not found")
			}
		}
	}
	return taskGraph, noNeedResult, nil
}

func (ex *Executor) ExecuteConcurrency(ctx context.Context, tasks []Task, results chan TaskResult) error {
	taskGraph, noNeedResult, err := ex.resolveTaskDeps(tasks)
	if err != nil {
		return err
	}

	wg := new(sync.WaitGroup)
	for _, task := range taskGraph {
		wg.Add(1)

		go func(task Task) {
			ex.execute(task, wg, ctx, noNeedResult, results)
		}(task)
	}
	wg.Wait()
	close(results)
	return nil
}

func (ex *Executor) execute(task Task, wg *sync.WaitGroup, ctx context.Context, noNeedResult map[string]struct{}, results chan TaskResult) {
	defer wg.Done()

	var deps []TaskResult

	for i := len(task.DepNames()); i > 0; i-- {
		depData := <-task.DepResultsChan()
		deps = append(deps, depData)
		ctx = context.WithValue(ctx, depData.GetName()+"_ret", depData)
	}

	// deps done
	// fmt.Printf("%s deps: %+v task done\n", task.Name, deps)

	data, err := task.Do(ctx)
	r := &Result{
		Name: task.Name(),
		Data: data,
		Err:  err,
	}

	if _, ok := noNeedResult[task.Name()]; !ok {
		results <- r
	}

	for _, ch := range task.Children() {
		ch <- r
	}
}

func (ex *Executor) Execute(ctx context.Context, taskList []Task, results map[string]TaskResult) error {
	var tasks = map[string]Task{}

	for _, task := range taskList {
		tasks[task.Name()] = task
	}

	return ex.doTask(tasks, ctx, results)
}

func (ex *Executor) doTask(tasks map[string]Task, ctx context.Context, results map[string]TaskResult) error {
	undoTasks := map[string]Task{}
	for _, task := range tasks {
		// well ! no deps
		if len(task.DepNames()) == 0 {
			i, err := task.Do(ctx)
			results[task.Name()+"_ret"] = &Result{
				Name: task.Name(),
				Data: i,
				Err:  err,
			}
		} else {
			var deps []TaskResult
			for _, name := range task.DepNames() {
				_, inResult := results[name+"_ret"]
				_, inTasks := tasks[name]
				if !inResult && !inTasks {
					if dep, inRegistry := ex.registry[name]; inRegistry {
						tasks[name] = dep
					} else {
						return errors.Errorf("%s deps on %s not found", task.Name(), name)
					}
				}

				v, ok := results[name+"_ret"]
				if ok {
					deps = append(deps, v)
				}
			}

			if len(deps) < len(task.DepNames()) {
				// deps not enough!
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

				results[task.Name()+"_ret"] = &Result{
					Name: task.Name(),
					Data: i,
					Err:  err,
				}
			}
		}
	}

	if len(undoTasks) > 0 {
		err := ex.doTask(undoTasks, ctx, results)
		if err != nil {
			return err
		}
	}
	return nil
}

type Task interface {
	Do(ctx context.Context) (interface{}, error)
	Name() string
	DepNames() []string
	DepResultsChan() chan TaskResult
	Children() []chan TaskResult
	AddChild(chan TaskResult)
	GetInput() interface{}
	SetInput(i interface{})
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
