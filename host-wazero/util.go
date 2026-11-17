package wazero_cluster

func tagged(m map[string]string, tag, val string) bool {
	v, ok := m[tag]
	return ok && (val == "" || v == val)
}
