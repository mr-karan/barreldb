package barrel

type Meta struct {
	Timestamp  int
	RecordSize int
	RecordPos  int
	FileID     int
}

type KeyDir map[string]Meta
