package storage

import (
	"context"

	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
)

// Snapshot 磁盘快照容器，Version 用于字段增删后的兼容读取。
type Snapshot struct {
	Version int          `json:"version"`
	State   domain.State `json:"state"`
}

// Store 本地状态存储：WAL 操作日志 + 快照。
type Store interface {
	// LoadLatest 读取最近一次快照（无快照返回 nil,nil）。
	LoadLatest(ctx context.Context) (*Snapshot, error)
	// SaveSnapshot 原子写入快照（临时文件 + rename）。
	SaveSnapshot(ctx context.Context, snap *Snapshot) error
	// AppendOp 追加一条操作日志。
	AppendOp(ctx context.Context, op Op) error
	// ReadOps 顺序读取全部操作日志。
	ReadOps(ctx context.Context) ([]Op, error)
	// TruncateOps 在生成快照后清空日志。
	TruncateOps(ctx context.Context) error
}
