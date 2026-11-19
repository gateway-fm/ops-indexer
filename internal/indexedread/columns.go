package indexedread

// ReadColumns returns detached, sorted columns from the same validated embedded
// descriptor used by the serving reader. Offline owners may use it for minimum
// grants; it executes no SQL and accepts no caller-selected descriptor.
func ReadColumns() (map[string][]string, error) {
	definition, err := compiledDefinition()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string, len(definition.Tables))
	for _, table := range definition.Tables {
		columns := []string{}
		for _, column := range table.Columns {
			if column.Select {
				columns = append(columns, column.Name)
			}
		}
		out[table.Name] = columns
	}
	return out, nil
}
