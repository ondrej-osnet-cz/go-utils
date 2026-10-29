package backup

import "time"

// SetClock replaces the clock that names backups.
func SetClock(b *Backuper, now func() time.Time) { b.now = now }
