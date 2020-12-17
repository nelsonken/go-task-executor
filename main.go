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

func DoThings(xs []*Task) error {
	// commonTask For common Data
	var commonTasks = map[string]*Task{
		"common_task_1": &Task{F: func(ctx context.Context) interface{} {
			fmt.Println("common 1")
			return "c1"
		}, Name: "common1"},
		"common_task_2": &Task{F: func(ctx context.Context) interface{} {
			fmt.Println("common 2")
			return "c2"
		}, Name: "common2"},
	}

	var m = map[string]*Task{
	}

	for k, v := range commonTasks {
		m[k] = v
	}

	for _, x := range xs {
		if len(x.DependNames) > 0 {
			x.DependsResults = make(chan interface{}, len(x.DependNames)+1)
		}

		m[x.Name] = x
	}

	for _, x := range xs {
		for _, name := range x.DependNames {
			xx, ok := m[name]
			if ok {
				if xx.Children == nil {
					xx.Children = []chan interface{}{}
				}

				xx.Children = append(xx.Children, x.DependsResults)
			} else if xx, ok = registry[name]; ok {
				if xx.Children == nil {
					xx.Children = []chan interface{}{}
				}

				xx.Children = append(xx.Children, x.DependsResults)
			} else {
				// you can get from registry
				return errors.New("dependency " + name + " not found")
			}
		}
	}

	wg := new(sync.WaitGroup)
	for _, v := range m {
		wg.Add(1)
		go func(v *Task) {
			defer wg.Done()
			ctx := context.Background()
			deps := []interface{}{}
			for i := len(v.DependNames); i > 0; i-- {
				depData := <-v.DependsResults
				deps = append(deps, depData)
			}
			fmt.Printf("%s deps: %+v done", v.Name, deps)
			data := v.F(ctx)
			for _, ch := range v.Children {
				ch <- data
			}
		}(v)
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
			DependNames: []string{"d"},
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
