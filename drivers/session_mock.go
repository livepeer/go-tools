package drivers

import (
	"context"
	"io"
	"time"

	"github.com/stretchr/testify/mock"
)

type MockOSSession struct {
	mock.Mock
	waitForCh bool
	waitCh    chan struct{}
	back      chan struct{}
}

func NewMockOSSession() *MockOSSession {
	return &MockOSSession{
		waitCh: make(chan struct{}),
		back:   make(chan struct{}),
	}
}

func (s *MockOSSession) SaveData(ctx context.Context, name string, data io.Reader, fields *FileProperties, timeout time.Duration) (*SaveDataOutput, error) {
	args := s.Called(name, data, fields, timeout)
	if s.waitForCh {
		s.back <- struct{}{}
		<-s.waitCh
		s.waitForCh = false
	}
	return &SaveDataOutput{URL: args.String(0)}, args.Error(1)
}

func (s *MockOSSession) EndSession() {
	s.Called()
}

func (s *MockOSSession) GetInfo() *OSInfo {
	args := s.Called()
	if args.Get(0) != nil {
		return args.Get(0).(*OSInfo)
	}
	return nil
}

func (s *MockOSSession) IsExternal() bool {
	args := s.Called()
	return args.Bool(0)
}

func (s *MockOSSession) IsOwn(url string) bool {
	args := s.Called()
	return args.Bool(0)
}

func (s *MockOSSession) ListFiles(ctx context.Context, prefix, delim string) (PageInfo, error) {
	return nil, nil
}

func (s *MockOSSession) DeleteFile(ctx context.Context, name string) error {
	return nil
}

func (s *MockOSSession) ReadData(ctx context.Context, name string) (*FileInfoReader, error) {
	args := s.Called(ctx, name)
	var fi *FileInfoReader
	if args.Get(0) != nil {
		fi = args.Get(0).(*FileInfoReader)
	}
	return fi, args.Error(1)
}
func (s *MockOSSession) OS() OSDriver {
	return nil
}

func (s *MockOSSession) Presign(name string, expire time.Duration) (string, error) {
	return "", ErrNotSupported
}

func (s *MockOSSession) ReadDataRange(ctx context.Context, name, byteRange string) (*FileInfoReader, error) {
	return nil, ErrNotSupported
}
