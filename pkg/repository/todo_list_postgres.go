package repository

import (
	"fmt"
	todo "github.com/daddyrusher/rest-api-sample"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"strings"
)

type TodoListPostgres struct {
	db *sqlx.DB
}

func NewTodoListPostgres(db *sqlx.DB) *TodoListPostgres {
	return &TodoListPostgres{db: db}
}

func (r *TodoListPostgres) Create(userId int, list todo.TodoList) (int, error) {
	tx, err := r.db.Begin()

	if err != nil {
		return 0, nil
	}

	var listId int
	row := tx.QueryRow("INSERT INTO $1 (title, description) values ($2, $3) RETURNING id",
		todoListsTable, list.Title, list.Description)

	err = row.Scan(&listId)
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	_, err = tx.Exec("INSERT INTO $1 (user_id, list_id) values ($2, $3)", usersListsTable, userId, listId)
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	return listId, tx.Commit()
}

func (r *TodoListPostgres) GetAll(userId int) ([]todo.TodoList, error) {
	var lists []todo.TodoList
	err := r.db.Select(&lists, "SELECT tl.id, tl.title, tl.description FROM $1 tl INNER JOIN $2 ul on tl.id = ul.list_id where ul.user_id = $3",
		todoListsTable, usersListsTable, userId)

	return lists, err
}

func (r *TodoListPostgres) GetById(userId, listId int) (todo.TodoList, error) {
	var list todo.TodoList
	err := r.db.Get(&list, `SELECT tl.id, tl.title, tl.description FROM $1 tl INNER JOIN $2 ul
                                       on tl.id = ul.list_id where ul.user_id = $3 and ul.list_id = $4`,
		todoListsTable, usersListsTable, userId, listId)

	return list, err
}

func (r *TodoListPostgres) DeleteById(userId int, listId int) error {
	_, err := r.db.Exec("DELETE FROM $1 tl USING $2 ul WHERE tl.id = ul.list_id and ul.user_id = $3 and ul.list_id = $4",
		todoListsTable, usersListsTable, userId, listId)

	return err
}

func (r *TodoListPostgres) UpdateById(userId int, listId int, input todo.UpdateListInput) error {
	setValues := make([]string, 0)
	args := make([]interface{}, 0)
	argId := 1

	if input.Title != nil {
		setValues = append(setValues, fmt.Sprintf("title=$%d", argId))
		args = append(args, input.Title)
		argId++
	}

	if input.Description != nil {
		setValues = append(setValues, fmt.Sprintf("description=$%d", argId))
		args = append(args, input.Description)
		argId++
	}

	setQuery := strings.Join(setValues, ", ")

	query := fmt.Sprintf(`UPDATE %s tl SET %s FROM %s ul where tl.id = ul.list_id and ul.list_id = $%d
                                   and ul.user_id = $%d`, todoListsTable, setQuery, usersListsTable, argId, argId+1)

	args = append(args, listId, userId)

	logrus.Debugf("updateQuery: %s", query)
	logrus.Debugf("args: %s", args)

	_, err := r.db.Exec(query, args...)

	return err
}
