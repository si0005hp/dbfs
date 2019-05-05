package main

import (
	"database/sql"
	"fmt"

	"github.com/magiconair/properties"
)

// FRM ...
type FRM interface {
	/* R -> F */
	MapToFile(rs *sql.Rows) ([][]byte, error)
	/* F -> R */
	MapToUpdate(f *RowFile, data []byte) (string, []interface{}, error)
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

// MapToUpdate ...
func (m *PropertiesMapper) MapToUpdate(f *RowFile, data []byte) (*Update, error) {
	np := properties.MustLoadString(string(data))
	op := properties.MustLoadString(string(f.data))

	// Extract changed cols only
	updCols := make(map[string]interface{})
	for k, nv := range np.Map() {
		ov, ok := op.Get(k)
		if ok && ov != nv {
			updCols[k] = nv
		}
	}
	if len(updCols) <= 0 {
		return nil, nil
	}

	// Extract pk cols for building where phrase
	pkCols := make(map[string]interface{})
	for _, pk := range f.parent.meta.pkCols {
		v, ok := op.Get(pk)
		if !ok {
			return nil, fmt.Errorf("%s: pk %s is not found", f.parent.meta.tblNm, pk)
		}
		pkCols[pk] = v
	}

	return &Update{
		tbl:      f.parent.meta.tblNm,
		setCls:   updCols,
		whereCls: pkCols,
	}, nil
}
