package dispatchtest

import "os"
import "path/filepath"

func writeSnapshotFile(dir, content string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "snapshot.json"), []byte(content), 0o644)
}
