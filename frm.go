package main

import (
	"database/sql"

	"github.com/magiconair/properties"
)

// FRM ...
type FRM interface {
	MapToFile(rs *sql.Rows) ([][]byte, error)
}

// PropertiesMapper ...
type PropertiesMapper struct {
}

// NewPropertiesMapper ...
func NewPropertiesMapper() *PropertiesMapper {
	return &PropertiesMapper{}
}

// MapToFile ...
func (m *PropertiesMapper) MapToFile(rs *sql.Rows) ([][]byte, error) {
	cs, err := rs.Columns()
	if err != nil {
		return nil, err
	}

	bs := make([][]byte, 0)
	for rs.Next() {
		// Scan row
		vals := make([]interface{}, len(cs))
		for i := range vals {
			var s string
			vals[i] = &s
		}
		err = rs.Scan(vals...)
		if err != nil {
			return nil, err
		}
		// Generate file data
		pm := make(map[string]string, len(cs))
		for i, p := range vals {
			pm[cs[i]] = *(p.(*string))
		}
		b := []byte(properties.LoadMap(pm).String())
		bs = append(bs, b)
	}
	return bs, nil
}
