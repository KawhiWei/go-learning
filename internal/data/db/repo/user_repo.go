package repo

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/luck/go-learning/internal/biz"
	"github.com/luck/go-learning/internal/data/db/model"
)

type UserRepository struct {
	pool *pgxpool.Pool
}

func NewUserRepository(pool *pgxpool.Pool) *UserRepository {
	return &UserRepository{pool: pool}
}

func (r *UserRepository) Create(ctx context.Context, user *biz.User) (*biz.User, error) {
	const query = `
		INSERT INTO users (id, name, email)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING
		RETURNING id, name, email, created_at`

	stored := toUserModel(user)
	if stored.ID == uuid.Nil {
		stored.ID = uuid.New()
	}
	created := &model.User{}
	err := r.pool.QueryRow(ctx, query, stored.ID, stored.Name, stored.Email).Scan(
		&created.ID, &created.Name, &created.Email, &created.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// 同一 Kafka 事件可能在 Handler 成功但 offset 提交前重投。相同 ID、
		// 相同内容表示上一次写入已经成功，可以作为幂等成功返回；相同 ID
		// 不同内容则是数据冲突，不能静默覆盖。
		existing, getErr := r.Get(ctx, stored.ID)
		if getErr != nil {
			return nil, getErr
		}
		if existing.Name == stored.Name && existing.Email == stored.Email {
			return existing, nil
		}
		return nil, biz.ErrAlreadyExists
	}
	if err != nil {
		if isUniqueViolation(err) {
			return nil, biz.ErrAlreadyExists
		}
		return nil, err
	}
	return toBizUser(created), nil
}

func (r *UserRepository) Get(ctx context.Context, id uuid.UUID) (*biz.User, error) {
	const query = `SELECT id, name, email, created_at FROM users WHERE id = $1`
	user := &model.User{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&user.ID, &user.Name, &user.Email, &user.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return toBizUser(user), nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
