package service

import (
	todo "github.com/daddyrusher/rest-api-sample"
	"github.com/daddyrusher/rest-api-sample/pkg/repository"
)

type TodoListService struct {
	repo repository.TodoList
}

func NewTodoListService(repo repository.TodoList) *TodoListService {
	return &TodoListService{repo: repo}
}

func (s *TodoListService) Create(userId int, list todo.TodoList) (int, error) {
	return s.repo.Create(userId, list)
}

func (s *TodoListService) GetAll(userId int) ([]todo.TodoList, error) {
	return s.repo.GetAll(userId)
}

func (s *TodoListService) GetById(userId, listId int) (todo.TodoList, error) {
	return s.repo.GetById(userId, listId)
}

func (s *TodoListService) DeleteById(userId, listId int) error {
	return s.repo.DeleteById(userId, listId)
}

func (s *TodoListService) UpdateById(userId, listId int, input todo.UpdateListInput) error {
	if err := input.Validate(); err != nil {
		return err
	}

	return s.repo.UpdateById(userId, listId, input)
}
