package storage

import (
	"context"
	"io"
)

type Storage interface {
	Write(ctx context.Context, filepath string, data io.Reader) error
	Read(ctx context.Context, filepath string) (io.ReadCloser, error)
	List(ctx context.Context, prefix string) ([]string, error)
}
