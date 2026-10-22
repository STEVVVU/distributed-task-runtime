package cluster

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
)

const MaxAttempts = 3

type Arg struct {
	Kind   string         `json:"kind"`
	Value  any            `json:"value,omitempty"`
	Ref    string         `json:"ref,omitempty"`
	Items  []Arg          `json:"items,omitempty"`
	Fields map[string]Arg `json:"fields,omitempty"`
}
type Task struct {
	ID       string         `json:"id"`
	Module   string         `json:"module"`
	Function string         `json:"function"`
	Args     []Arg          `json:"args"`
	Kwargs   map[string]Arg `json:"kwargs"`
	Deps     []string       `json:"deps"`
	State    string         `json:"state"`
	Attempt  int            `json:"attempt"`
	Worker   string         `json:"worker"`
	Error    string         `json:"error"`
	Created  int64          `json:"created"`
	Started  int64          `json:"started"`
}
type Location struct {
	Address string `json:"address"`
	Key     string `json:"key"`
}
type Object struct {
	ID        string              `json:"id"`
	Root      json.RawMessage     `json:"root,omitempty"`
	Locations map[string]Location `json:"locations"`
}
type Node struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	Last      int64  `json:"last"`
	Busy      string `json:"busy"`
	Executed  int    `json:"executed"`
	Transfers int    `json:"transfers"`
}
type State struct {
	Tasks   map[string]*Task   `json:"tasks"`
	Objects map[string]*Object `json:"objects"`
	Nodes   map[string]*Node   `json:"nodes"`
}
type Request struct {
	Op        string          `json:"op"`
	ID        string          `json:"id"`
	Value     json.RawMessage `json:"value"`
	Task      *Task           `json:"task"`
	Worker    string          `json:"worker"`
	Address   string          `json:"address"`
	Attempt   int             `json:"attempt"`
	Error     string          `json:"error"`
	Transfers int             `json:"transfers"`
	Key       string          `json:"key"`
	Retry     bool            `json:"retry"`
}
type Spec struct {
	Root      json.RawMessage     `json:"root,omitempty"`
	Locations map[string]Location `json:"locations"`
}
type Assignment struct {
	Task   *Task           `json:"task,omitempty"`
	Inputs map[string]Spec `json:"inputs,omitempty"`
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_-]{1,100}$`)
var symbol = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

func dependencies(a Arg, out map[string]bool, depth int) error {
	if depth > 32 {
		return errors.New("argument nesting exceeds 32")
	}
	switch a.Kind {
	case "literal":
		if a.Ref != "" || len(a.Items) > 0 || len(a.Fields) > 0 {
			return errors.New("invalid literal")
		}
		return validValue(a.Value, depth)
	case "ref":
		if !identifier.MatchString(a.Ref) || a.Value != nil || len(a.Items) > 0 || len(a.Fields) > 0 {
			return errors.New("invalid reference")
		}
		out[a.Ref] = true
	case "list":
		if a.Value != nil || a.Ref != "" || len(a.Fields) > 0 {
			return errors.New("invalid list argument")
		}
		for _, child := range a.Items {
			if err := dependencies(child, out, depth+1); err != nil {
				return err
			}
		}
	case "dict":
		if a.Value != nil || a.Ref != "" || len(a.Items) > 0 {
			return errors.New("invalid dictionary argument")
		}
		for _, child := range a.Fields {
			if err := dependencies(child, out, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid argument kind")
	}
	return nil
}

func validValue(value any, depth int) error {
	if depth > 32 {
		return errors.New("value nesting exceeds 32")
	}
	switch v := value.(type) {
	case nil, string, bool:
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > 9007199254740991 {
			return errors.New("number outside finite exact range")
		}
	case []any:
		for _, child := range v {
			if err := validValue(child, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, child := range v {
			if err := validValue(child, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid JSON value")
	}
	return nil
}
