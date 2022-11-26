package barrel

type Meta struct {
	Timestamp  int
	RecordSize int
	RecordPos  int
	FileID     string
}

type KeyDir map[string]Meta
