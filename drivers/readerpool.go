package drivers

import (
	"context"
	"io/ioutil"
)

type readResult struct {
	index    int
	fileInfo *FileInfoReader
	data     []byte
	err      error
}

type task struct {
	sess     OSSession
	fileName string
	index    int
}

func readWorker(ctx context.Context, tasks chan *task, resCh chan *readResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-tasks:
			res := &readResult{
				index: task.index,
			}
			fi, err := task.sess.ReadData(ctx, task.fileName)
			if err != nil {
				res.err = err
				resCh <- res
				continue
			}
			fb, err := ioutil.ReadAll(fi.Body)
			if err != nil {
				res.err = err
				resCh <- res
				continue
			}
			fi.Body.Close()
			res.data = fb
			res.fileInfo = fi
			resCh <- res
		}
	}
}

// ParallelReadFiles reads files in parallel, using specified number of jobs
func ParallelReadFiles(ctx context.Context, sess OSSession, filesNames []string, workers int) ([]*FileInfoReader, [][]byte, error) {
	workersToStart := workers
	if len(filesNames) < workers {
		workersToStart = len(filesNames)
	}
	resCh := make(chan *readResult, len(filesNames))
	tasks := make(chan *task)
	for i := 0; i < workersToStart; i++ {
		go readWorker(ctx, tasks, resCh)
	}
	for i, fn := range filesNames {
		task := &task{
			fileName: fn,
			sess:     sess,
			index:    i,
		}
		tasks <- task
	}
	firs := make([]*FileInfoReader, len(filesNames))
	data := make([][]byte, len(filesNames))
	var err error
	for i := 0; i < len(filesNames); i++ {
		res := <-resCh
		firs[res.index] = res.fileInfo
		data[res.index] = res.data
		if res.err != nil {
			err = res.err
		}
	}
	return firs, data, err
}
