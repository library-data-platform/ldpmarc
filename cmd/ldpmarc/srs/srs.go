// Package srs transforms FOLIO SRS MARC records in JSON format into a tabular
// form.
package srs

import (
	"encoding/json"
	"fmt"
)

// Marc is a single "row" of data extracted from part of a MARC record.
type Marc struct {
	Line    int64
	Field   string
	Ind1    string
	Ind2    string
	Ord     int64
	SF      string
	Content string
}

// Transform converts marcjson, an SRS MARC record in JSON format, into a
// table.  Only a MARC record considered to be current is transformed, where
// current is defined as having state = "ACTUAL" and some content present in
// 999$i which is presumed to be the FOLIO instance identifer.  Transform
// returns the resultant table as a slice of Marc structs and the instance
// identifer as a string.  If the MARC record is not current, Transform returns
// an empty slice and the instance identifier as "".
func Transform(marcjson string, state string) ([]Marc, string, error) {
	// mrecs is the slice of Marc structs that will contain the transformed
	// rows.
	mrecs := make([]Marc, 0)
	// Convert the JSON object into a map[string]interface{} which will be
	// used to extract all of the required data from the MARC record.
	var err error
	var i interface{}
	if err = json.Unmarshal([]byte(marcjson), &i); err != nil {
		return nil, "", err
	}
	var ok bool
	var m map[string]interface{}
	if m, ok = i.(map[string]interface{}); !ok {
		return nil, "", fmt.Errorf("parsing error")
	}
	// Extract the leader.
	var leader string
	if leader, err = getLeader(m); err != nil {
		return nil, "", fmt.Errorf("parsing: %s", err)
	}
	// Extract the "fields" array.
	if i, ok = m["fields"]; !ok {
		return nil, "", fmt.Errorf("parsing: \"fields\" not found")
	}
	var a []interface{}
	if a, ok = i.([]interface{}); !ok {
		return nil, "", fmt.Errorf("parsing: \"fields\" is not an array")
	}
	// Each element of the fields array is an object (map) with a MARC tag
	// and possibly subfields.
	var line int64 = 1
	var fieldCounts = make(map[string]int64)
	for _, i = range a {
		if m, ok = i.(map[string]interface{}); !ok {
			return nil, "", fmt.Errorf("parsing: \"fields\" element is not an object")
		}
		var t string
		var ii interface{}
		for t, ii = range m {
			var fieldC int64 = fieldCounts[t] + 1
			fieldCounts[t] = fieldC
			switch v := ii.(type) {
			case string:
				// We convert a string field to a single row of output.
				if t == "001" {
					// When we encounter 001, we first output the
					// leader as 000.
					mrecs = append(mrecs, Marc{
						Line:    line,
						Field:   "000",
						Ind1:    "",
						Ind2:    "",
						Ord:     fieldC,
						SF:      "",
						Content: leader,
					})
					line++
				}
				// Now write the row.
				mrecs = append(mrecs, Marc{
					Line:    line,
					Field:   t,
					Ind1:    "",
					Ind2:    "",
					Ord:     fieldC,
					SF:      "",
					Content: v,
				})
				line++
			case map[string]interface{}:
				// An object (map) needs further processing.
				// We call transformSubfields which will output
				// one or more rows to mrecs.
				if err = transformSubfields(&mrecs, &line, t, fieldC, v); err != nil {
					return nil, "", fmt.Errorf("parsing: %s", err)
				}
			default:
				return nil, "", fmt.Errorf("parsing: unknown data type in field \"" + t + "\"")
			}

		}
	}
	// Extract the instance identifier from 999$i.
	var instanceID string = getInstanceID(mrecs)
	// If the MARC record is not current, return nothing.
	if !isCurrent(state, instanceID) {
		return []Marc{}, "", nil
	}
	return mrecs, instanceID, nil
}

func transformSubfields(mrecs *[]Marc, line *int64, field string, ord int64, sm map[string]interface{}) error {
	var ok bool
	var i interface{}
	// Ind1
	if i, ok = sm["ind1"]; !ok {
		return fmt.Errorf("parsing: \"ind1\" not found")
	}
	var ind1 string
	if ind1, ok = i.(string); !ok {
		return fmt.Errorf("parsing: \"ind1\" wrong type")
	}
	// Ind2
	if i, ok = sm["ind2"]; !ok {
		return fmt.Errorf("parsing: \"ind2\" not found")
	}
	var ind2 string
	if ind2, ok = i.(string); !ok {
		return fmt.Errorf("parsing: \"ind2\" wrong type")
	}
	// Subfields
	if i, ok = sm["subfields"]; !ok {
		return fmt.Errorf("parsing: \"subfields\" not found")
	}
	var a []interface{}
	if a, ok = i.([]interface{}); !ok {
		return fmt.Errorf("parsing: \"subfields\" is not an array")
	}
	for _, i = range a {
		var m map[string]interface{}
		if m, ok = i.(map[string]interface{}); !ok {
			return fmt.Errorf("parsing: \"subfields\" element is not an object")
		}
		var k string
		var v interface{}
		for k, v = range m {
			var vs string
			if vs, ok = v.(string); !ok {
				return fmt.Errorf("parsing: subfield value is not a string")
			}
			*mrecs = append(*mrecs, Marc{
				Line:    *line,
				Field:   field,
				Ind1:    ind1,
				Ind2:    ind2,
				Ord:     ord,
				SF:      k,
				Content: vs,
			})
			*line++
		}
	}

	return nil
}

func getLeader(m map[string]interface{}) (string, error) {
	var i interface{}
	var ok bool
	if i, ok = m["leader"]; !ok {
		return "", fmt.Errorf("\"leader\" not found")
	}
	var s string
	if s, ok = i.(string); !ok {
		return "", fmt.Errorf("\"leader\" is not a string")
	}
	return s, nil
}

func getInstanceID(mrecs []Marc) string {
	var rec Marc
	for _, rec = range mrecs {
		if rec.Field == "999" && rec.Ind1 == "f" && rec.Ind2 == "f" && rec.SF == "i" && rec.Content != "" {
			return rec.Content
		}
	}
	return ""
}

func isCurrent(state string, instanceID string) bool {
	return state == "ACTUAL" && instanceID != ""
}
