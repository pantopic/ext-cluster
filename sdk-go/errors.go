package cluster

type strErr string

func (e strErr) Error() string {
	return string(e)
}
