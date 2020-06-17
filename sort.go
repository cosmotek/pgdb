package pgdb

// MigrationSet is simply an Migration slice, used for sorting migrations in numbered order.
type MigrationSet []Migration

// Len is a method provided to satisfy the sort interface.
func (s MigrationSet) Len() int {
	return len(s)
}

// Swap is a method provided to satisfy the sort interface.
func (s MigrationSet) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less is a method provided to satisfy the sort interface.
func (s MigrationSet) Less(i, j int) bool {
	return s[i].Version < s[j].Version
}
