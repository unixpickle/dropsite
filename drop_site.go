package dropsite

// A DropSite represents an online location where data can be uploaded and retrieved.
type DropSite interface {
	Download() ([]byte, error)
	Upload([]byte) error
}
