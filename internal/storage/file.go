package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	snapshotName = "snapshot.json"
	opsName      = "ops.log"
)

// FileStore 本地文件存储：snapshot.json（临时文件 + rename 原子写），
// ops.log（append JSONL，按行落盘）。
type FileStore struct {
	dir string
}

func NewFileStore(dir string) *FileStore { return &FileStore{dir: dir} }

func (f *FileStore) path(name string) string { return filepath.Join(f.dir, name) }

func (f *FileStore) LoadLatest(ctx context.Context) (*Snapshot, error) {
	data, err := os.ReadFile(f.path(snapshotName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func (f *FileStore) SaveSnapshot(ctx context.Context, snap *Snapshot) error {
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path(snapshotName + ".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path(snapshotName))
}

func (f *FileStore) AppendOp(ctx context.Context, op Op) error {
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return err
	}
	fp, err := os.OpenFile(f.path(opsName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer fp.Close()
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	if _, err := fp.Write(append(data, '\n')); err != nil {
		return err
	}
	return fp.Sync()
}

func (f *FileStore) ReadOps(ctx context.Context) ([]Op, error) {
	fp, err := os.Open(f.path(opsName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	var ops []Op
	sc := bufio.NewScanner(fp)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var op Op
		if err := json.Unmarshal(line, &op); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, sc.Err()
}

func (f *FileStore) TruncateOps(ctx context.Context) error {
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return err
	}
	tmp := f.path(opsName + ".tmp")
	if err := os.WriteFile(tmp, []byte{}, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path(opsName))
}
