package storage

import (
	"context"
	"sync"
)

// MemoryStore 纯内存存储，用于测试与单机演示。
type MemoryStore struct {
	mu   sync.Mutex
	snap *Snapshot
	ops  []Op
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (m *MemoryStore) LoadLatest(ctx context.Context) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snap == nil {
		return nil, nil
	}
	cp := *m.snap
	return &cp, nil
}

func (m *MemoryStore) SaveSnapshot(ctx context.Context, snap *Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *snap
	m.snap = &cp
	return nil
}

func (m *MemoryStore) AppendOp(ctx context.Context, op Op) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, op)
	return nil
}

func (m *MemoryStore) ReadOps(ctx context.Context) ([]Op, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Op(nil), m.ops...), nil
}

func (m *MemoryStore) TruncateOps(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = nil
	return nil
}
