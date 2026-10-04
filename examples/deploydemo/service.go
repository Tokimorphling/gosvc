package deploydemo

import (
	"context"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Tokimorphling/gosvc/apierror"
)

type Task struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	Done      bool      `json:"done"`
	CreatedAt time.Time `json:"createdAt"`
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

// Service owns business state, independent of HTTP and JSON-RPC. Writes are
// serialized and persisted before success is returned. Use one process per
// data file; this small sample deliberately does not implement a database.
type Service struct {
	mu     sync.RWMutex
	state  diskState
	config TaskConfig
}

func NewService(cfg TaskConfig) (*Service, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	state, err := loadState(cfg)
	if err != nil {
		return nil, err
	}
	return &Service{state: state, config: cfg}, nil
}

func (s *Service) Create(ctx context.Context, req CreateRequest) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	title := strings.TrimSpace(req.Title)
	if !validTitle(title, s.config.MaxTitleLen) {
		return nil, apierror.Newf(apierror.KindInvalidArgument, "title must contain 1 to %d valid UTF-8 characters", s.config.MaxTitleLen)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.state.Tasks) >= s.config.MaxTasks || s.state.NextID == math.MaxInt64 {
		return nil, apierror.New(apierror.KindConflict, "task limit reached; delete a task first")
	}
	task := Task{ID: s.state.NextID + 1, Title: title, CreatedAt: time.Now().UTC()}
	next := s.state
	next.NextID = task.ID
	next.Tasks = append(slices.Clone(s.state.Tasks), task)
	if err := s.commit(ctx, next); err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *Service) Get(ctx context.Context, req IDRequest) (*Task, error) {
	if err := validateID(ctx, req.ID); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	index, ok := taskIndex(s.state.Tasks, req.ID)
	if !ok {
		return nil, taskNotFound(req.ID)
	}
	task := s.state.Tasks[index]
	return &task, nil
}

func (s *Service) List(ctx context.Context, _ struct{}) ([]Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]Task, len(s.state.Tasks))
	copy(items, s.state.Tasks)
	return items, nil
}

// Complete is idempotent and avoids a disk write when already completed.
func (s *Service) Complete(ctx context.Context, req IDRequest) (*Task, error) {
	if err := validateID(ctx, req.ID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index, ok := taskIndex(s.state.Tasks, req.ID)
	if !ok {
		return nil, taskNotFound(req.ID)
	}
	task := s.state.Tasks[index]
	if task.Done {
		return &task, nil
	}
	task.Done = true
	next := s.state
	next.Tasks = slices.Clone(s.state.Tasks)
	next.Tasks[index] = task
	if err := s.commit(ctx, next); err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *Service) Delete(ctx context.Context, req IDRequest) (DeleteResponse, error) {
	if err := validateID(ctx, req.ID); err != nil {
		return DeleteResponse{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index, ok := taskIndex(s.state.Tasks, req.ID)
	if !ok {
		return DeleteResponse{}, taskNotFound(req.ID)
	}
	next := s.state
	next.Tasks = slices.Delete(slices.Clone(s.state.Tasks), index, index+1)
	if err := s.commit(ctx, next); err != nil {
		return DeleteResponse{}, err
	}
	return DeleteResponse{Deleted: true}, nil
}

// commit requires s.mu. A successful rename is visible immediately, including
// the unusual case where syncing the directory subsequently reports an error.
func (s *Service) commit(ctx context.Context, next diskState) error {
	committed, err := saveState(ctx, s.config.DataFile, next)
	if committed {
		s.state = next
	}
	if err != nil {
		return apierror.Wrap(err, apierror.KindInternal, "persist tasks")
	}
	return nil
}

func validTitle(title string, maxLen int) bool {
	return title != "" && utf8.ValidString(title) && utf8.RuneCountInString(title) <= maxLen
}

func taskIndex(tasks []Task, id int64) (int, bool) {
	return slices.BinarySearchFunc(tasks, id, func(task Task, id int64) int {
		switch {
		case task.ID < id:
			return -1
		case task.ID > id:
			return 1
		default:
			return 0
		}
	})
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
