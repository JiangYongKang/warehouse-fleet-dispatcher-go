package dispatcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// SnapshotFormat 是持久化快照的当前格式版本。
const SnapshotFormat = "fleet-snapshot/v2"

// SnapshotV1 是历史格式标识：没有 format 字段的快照即视为 v1。
const SnapshotFormatV1 = "fleet-snapshot/v1"

// Snapshot 是内核的完整可恢复状态（JSON 持久化）。
type Snapshot struct {
	Format string `json:"format"`
	Clock  int64  `json:"clock"`
	Seq    int64  `json:"seq"`

	Robots []robotSnapshot `json:"robots"`
	Tasks  []taskSnapshot  `json:"tasks"`
	Resv   []resvSnapshot  `json:"reservations"`
}

// 持久化结构刻意独立于运行时结构：
// 缺失字段解码为零值、新增字段被旧版本忽略，从而字段增删不破坏可读性。
type robotSnapshot struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Node        string `json:"node"`
	CurrentTask string `json:"current_task,omitempty"`
}

type taskSnapshot struct {
	ID       string   `json:"id"`
	Priority int      `json:"priority"`
	From     string   `json:"from"`
	To       string   `json:"to"`
	Deps     []string `json:"deps,omitempty"`
	Status   string   `json:"status"`
	Assignee string   `json:"assignee,omitempty"`
	Attempt  int      `json:"attempt,omitempty"`
	Dispatch int      `json:"dispatch,omitempty"` // v2 新增：观测用分派次数；旧文件缺失取 0
	Seq      int64    `json:"seq"`
	// WaitTick 为 v2 新增字段；旧 v1 文件没有该键，解码取零值，不影响读取。
	WaitTick int64  `json:"wait_tick,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

type resvSnapshot struct {
	RobotID string `json:"robot_id"`
	TaskID  string `json:"task_id"`
	Kind    string `json:"kind"`
	ResID   string `json:"res_id"`
	Start   int64  `json:"start"`
	End     int64  `json:"end"`
}

// Store 是快照持久化接口，测试中可替换为内存实现。
type Store interface {
	Save(snap Snapshot) error
	Load() (Snapshot, error)
}

// MemoryStore 是进程内存储，供测试与不落盘场景使用；同样保证全有或全无。
type MemoryStore struct {
	mu     sync.Mutex
	buf    []byte
	loaded bool
}

// Save 将快照编码后整体替换内部缓冲。
func (m *MemoryStore) Save(snap Snapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fail("MemoryStore.Save", ErrInvalidArgument, "marshal snapshot: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buf = data
	m.loaded = true
	return nil
}

// Load 返回最近一次保存的快照；从未保存时返回 ErrNotFound。
func (m *MemoryStore) Load() (Snapshot, error) {
	m.mu.Lock()
	data := append([]byte(nil), m.buf...)
	loaded := m.loaded
	m.mu.Unlock()
	if !loaded {
		return Snapshot{}, fail("MemoryStore.Load", ErrNotFound, "no snapshot")
	}
	return parseSnapshot(data)
}

// FileStore 以"临时文件 + fsync + 原子改名"写入快照，杜绝半更新文件。
type FileStore struct {
	path string
}

// NewFileStore 创建文件存储。
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Path 返回快照文件路径。
func (s *FileStore) Path() string { return s.path }

// Save 原子写入快照：先写同目录临时文件并 fsync，再 rename 覆盖，
// 任何一步崩溃都只会留下旧文件或完整的新文件，不会出现半个 JSON。
func (s *FileStore) Save(snap Snapshot) error {
	if s.path == "" {
		return fail("FileStore.Save", ErrInvalidArgument, "empty snapshot path")
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fail("FileStore.Save", ErrInvalidArgument, "marshal snapshot: %v", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fail("FileStore.Save", ErrInvalidArgument, "mkdir %q: %v", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return fail("FileStore.Save", ErrInvalidArgument, "create temp: %v", err)
	}
	tmpName := tmp.Name()
	clean := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		clean()
		return fail("FileStore.Save", ErrInvalidArgument, "write temp: %v", err)
	}
	if err := tmp.Sync(); err != nil { // 数据落盘
		_ = tmp.Close()
		clean()
		return fail("FileStore.Save", ErrInvalidArgument, "fsync: %v", err)
	}
	if err := tmp.Close(); err != nil {
		clean()
		return fail("FileStore.Save", ErrInvalidArgument, "close temp: %v", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil { // 原子替换
		clean()
		return fail("FileStore.Save", ErrInvalidArgument, "rename: %v", err)
	}
	return nil
}

// Load 读取快照并做版本迁移；文件不存在返回 ErrNotFound。
func (s *FileStore) Load() (Snapshot, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Snapshot{}, fail("FileStore.Load", ErrNotFound, "snapshot %q not found", s.path)
		}
		return Snapshot{}, fail("FileStore.Load", ErrInvalidArgument, "read %q: %v", s.path, err)
	}
	return parseSnapshot(data)
}

// parseSnapshot 识别格式版本并迁移为当前 Snapshot。
// 兼容范围：
//   - v2（当前）：直接解码；缺字段取零值、多余字段忽略 —— 字段增删均可读；
//   - v1（无 format 字段）：任务无 seq（按出现顺序补齐），
//     预约资源为复合串 "node:<id>" / "edge:<a>-><b>"（拆分为 kind+res_id）。
func parseSnapshot(data []byte) (Snapshot, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return Snapshot{}, fail("parseSnapshot", ErrInvalidArgument, "empty snapshot")
	}
	var head struct {
		Format string `json:"format"`
	}
	if err := json.Unmarshal(trimmed, &head); err != nil {
		return Snapshot{}, fail("parseSnapshot", ErrInvalidArgument, "decode format: %v", err)
	}
	switch head.Format {
	case "", SnapshotFormatV1:
		return migrateV1(trimmed)
	case SnapshotFormat:
		var snap Snapshot
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		if err := dec.Decode(&snap); err != nil {
			return Snapshot{}, fail("parseSnapshot", ErrInvalidArgument, "decode v2: %v", err)
		}
		snap.Format = SnapshotFormat
		return snap, nil
	default:
		return Snapshot{}, fail("parseSnapshot", ErrInvalidArgument, "unknown snapshot format %q", head.Format)
	}
}

// v1 的 JSON 形状（历史版本，只读不再写出）：
//
//	{
//	  "clock": 12,
//	  "robots": [{"id":"r1","status":"online","node":"A","task":"t1"}],
//	  "tasks":  [{"id":"t1","priority":1,"from":"A","to":"B",
//	             "deps":["t0"],"status":"assigned","assignee":"r1","attempt":1}],
//	  "reservations": [{"robot_id":"r1","task_id":"t1","res":"edge:A->B",
//	                    "start":3,"end":4}]
//	}
type v1Snapshot struct {
	Clock  int64 `json:"clock"`
	Robots []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Node   string `json:"node"`
		Task   string `json:"task"`
	} `json:"robots"`
	Tasks []struct {
		ID       string   `json:"id"`
		Priority int      `json:"priority"`
		From     string   `json:"from"`
		To       string   `json:"to"`
		Deps     []string `json:"deps"`
		Status   string   `json:"status"`
		Assignee string   `json:"assignee"`
		Attempt  int      `json:"attempt"`
	} `json:"tasks"`
	Reservations []struct {
		RobotID string `json:"robot_id"`
		TaskID  string `json:"task_id"`
		Res     string `json:"res"`
		Start   int64  `json:"start"`
		End     int64  `json:"end"`
	} `json:"reservations"`
}

// migrateV1 把 v1 快照映射为 v2：
// 补 format/seq，拆分复合资源串，机器人 task 字段更名 current_task。
func migrateV1(data []byte) (Snapshot, error) {
	var old v1Snapshot
	if err := json.Unmarshal(data, &old); err != nil {
		return Snapshot{}, fail("migrateV1", ErrInvalidArgument, "decode v1: %v", err)
	}
	snap := Snapshot{Format: SnapshotFormat, Clock: old.Clock, Seq: int64(len(old.Tasks))}
	for _, r := range old.Robots {
		snap.Robots = append(snap.Robots, robotSnapshot{
			ID: r.ID, Status: r.Status, Node: r.Node, CurrentTask: r.Task,
		})
	}
	for i, t := range old.Tasks {
		snap.Tasks = append(snap.Tasks, taskSnapshot{
			ID: t.ID, Priority: t.Priority, From: t.From, To: t.To,
			Deps:   append([]string(nil), t.Deps...),
			Status: t.Status, Assignee: t.Assignee, Attempt: t.Attempt,
			Seq: int64(i), WaitTick: 0, // v1 无序号：按出现顺序还原 FIFO；无等待计时：从 0 老化
		})
	}
	for _, rv := range old.Reservations {
		kind, id, err := parseV1Resource(rv.Res)
		if err != nil {
			return Snapshot{}, err
		}
		snap.Resv = append(snap.Resv, resvSnapshot{
			RobotID: rv.RobotID, TaskID: rv.TaskID, Kind: string(kind),
			ResID: id, Start: rv.Start, End: rv.End,
		})
	}
	return snap, nil
}

// parseV1Resource 解析 "node:<id>" / "edge:<a>-><b>" 复合串。
func parseV1Resource(s string) (ResourceKind, string, error) {
	switch {
	case strings.HasPrefix(s, "node:"):
		id := strings.TrimPrefix(s, "node:")
		if id == "" {
			return "", "", fail("parseV1Resource", ErrInvalidArgument, "empty node resource %q", s)
		}
		return ResNode, id, nil
	case strings.HasPrefix(s, "edge:"):
		id := strings.TrimPrefix(s, "edge:")
		if !strings.Contains(id, "->") {
			return "", "", fail("parseV1Resource", ErrInvalidArgument, "bad edge resource %q", s)
		}
		return ResEdge, id, nil
	default:
		return "", "", fail("parseV1Resource", ErrInvalidArgument, "unrecognized v1 resource %q", s)
	}
}

var _ Store = (*FileStore)(nil)
