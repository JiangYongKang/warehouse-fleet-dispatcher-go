package dispatcher

// persistLocked 将当前完整状态原子落盘（配置了 store 时）。
// 所有状态变更点都在同一把锁内、变更完成后调用本函数，
// 因而外部要么看到旧快照、要么看到新快照，不会出现半更新。
func (e *Engine) persistLocked() error {
	if e.store == nil {
		return nil
	}
	snap, err := e.buildSnapshotLocked()
	if err != nil {
		return err
	}
	if err := e.store.Save(snap); err != nil {
		return err
	}
	return nil
}
