package overrides

import (
	"encoding/json"
	"os"
)

type RoutingTable map[string]string

type RoutingTableRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func ParseRoutingTable(filepath string) (RoutingTable, error) {
	r := make([]RoutingTableRecord, 0)
	table := make(RoutingTable)
	f, err := os.Open(filepath)
	if err != nil {
		return table, err
	}
	defer f.Close()
	err = json.NewDecoder(f).Decode(&r)
	if err != nil {
		return table, err
	}
	for _, record := range r {
		table[record.Key] = record.Value
	}
	return table, nil
}
