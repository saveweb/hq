package queue

type Error struct {
	Code      string
	Message   string
	Retryable bool
	Details   map[string]any
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

type JobSpec struct {
	ID    string
	Value string
	Type  string
	Via   *string
	Hops  int
	Attrs map[string]any
}

type Outcome struct {
	Kind string
	Code *int
	URI  *string
	Meta map[string]any
}
type ExecutionError struct {
	Code    string
	Message string
	Details map[string]any
}
