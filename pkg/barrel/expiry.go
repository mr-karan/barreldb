package barrel

func (b *Barrel) CleanupExpired() error {
	b.Lock()
	defer b.Unlock()

	// Iterate over all keys and delete all keys which are expired.
	for k := range b.keydir {
		record, err := b.get(k)
		if err != nil {
			b.lo.Error("error fetching key", "key", k, "error", err)
			continue
		}
		if record.isExpired() {
			b.lo.Debug("deleting key since it's expired", "key", k)
			// Delete the key.
			if err := b.delete(k); err != nil {
				b.lo.Error("error deleting key", "key", k, "error", err)
				continue
			}
		}
	}

	return nil
}
