package barrel

type Meta struct {
	Timestamp  int
	RecordSize int
	FileID     string
}

type KeyDir map[string]Meta
