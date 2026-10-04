package tasks

import (
	"context"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/Tokimorphling/gosvc/apierror"
)

type Task struct {
	ID    int64  `json:"id"`
	Title string `json:"title"`
	Done  bool   `json:"done"`
}

type CreateRequest struct {
	Title string `json:"title"`
}
type IDRequest struct {
	ID int64 `json:"id"`
}
type DeleteResponse struct {
	Deleted bool `json:"deleted"`
}

// Service owns the business rules and an in-memory repository. Returning
// copies keeps caller mutations outside the shared state. Data is deliberately
// ephemeral in this sample: restarting the process clears every task.
type Service struct {
	mu     sync.RWMutex
	tasks  map[int64]Task
	nextID int64
	config TaskConfig
}

func NewService(cfg TaskConfig) *Service {
	return &Service{tasks: make(map[int64]Task), config: cfg}
}

func (s *Service) Create(ctx context.Context, req CreateRequest) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	title := strings.TrimSpace(req.Title)
	if title == "" || utf8.RuneCountInString(title) > s.config.MaxTitleLen {
		return nil, apierror.Newf(apierror.KindInvalidArgument, "title must contain 1 to %d characters", s.config.MaxTitleLen)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tasks) >= s.config.MaxTasks {
		return nil, apierror.New(apierror.KindConflict, "task limit reached; delete a task first")
	}
	s.nextID++
	task := Task{ID: s.nextID, Title: title}
	s.tasks[task.ID] = task
	return &task, nil
}

func (s *Service) Get(ctx context.Context, req IDRequest) (*Task, error) {
	if err := validateID(ctx, req.ID); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.tasks[req.ID]
	if !ok {
		return nil, taskNotFound(req.ID)
	}
	return &task, nil
}

func (s *Service) List(ctx context.Context, _ struct{}) ([]Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	items := make([]Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		items = append(items, task)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

// Complete is idempotent: repeating it leaves the task completed.
func (s *Service) Complete(ctx context.Context, req IDRequest) (*Task, error) {
	if err := validateID(ctx, req.ID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[req.ID]
	if !ok {
		return nil, taskNotFound(req.ID)
	}
	task.Done = true
	s.tasks[req.ID] = task
	return &task, nil
}

func (s *Service) Delete(ctx context.Context, req IDRequest) (DeleteResponse, error) {
	if err := validateID(ctx, req.ID); err != nil {
		return DeleteResponse{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[req.ID]; !ok {
		return DeleteResponse{}, taskNotFound(req.ID)
	}
	delete(s.tasks, req.ID)
	return DeleteResponse{Deleted: true}, nil
}

func validateID(ctx context.Context, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id <= 0 {
		return apierror.New(apierror.KindInvalidArgument, "id must be positive")
	}
	return nil
}

func taskNotFound(id int64) error {
	return apierror.Newf(apierror.KindNotFound, "task %d not found", id)
}
