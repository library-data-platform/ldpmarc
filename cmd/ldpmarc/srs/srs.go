package srs

import (
	"encoding/json"
	"fmt"
)

type Marc struct {
	Line    int64
	BibID   string
	Field   string
	Ind1    string
	Ind2    string
	Ord     int64
	SF      string
	Content string
}

func Transform(data string, state string) ([]Marc, string, error) {
	var mrecs = []Marc{}
	var err error
	var i interface{}
	if err = json.Unmarshal([]byte(data), &i); err != nil {
		return nil, "", err
	}
	var ok bool
	var m map[string]interface{}
	if m, ok = i.(map[string]interface{}); !ok {
		return nil, "", fmt.Errorf("parsing error")
	}
	// Parse leader
	var leader string
	if leader, err = parseLeader(m); err != nil {
		return nil, "", fmt.Errorf("parsing: %s", err)
	}
	// Fields
	if i, ok = m["fields"]; !ok {
		return nil, "", fmt.Errorf("parsing: \"fields\" not found")
	}
	var a []interface{}
	if a, ok = i.([]interface{}); !ok {
		return nil, "", fmt.Errorf("parsing: \"fields\" is not an array")
	}
	var line int64 = 1
	var bibID string
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
				if t == "001" {
					bibID = v
					// Leader (000)
					mrecs = append(mrecs, Marc{
						Line:    line,
						BibID:   bibID,
						Field:   "000",
						Ind1:    "",
						Ind2:    "",
						Ord:     fieldC,
						SF:      "",
						Content: leader,
					})
					line++
				}
				mrecs = append(mrecs, Marc{
					Line:    line,
					BibID:   bibID,
					Field:   t,
					Ind1:    "",
					Ind2:    "",
					Ord:     fieldC,
					SF:      "",
					Content: v,
				})
				line++
			case map[string]interface{}:
				if err = transformSubfields(&mrecs, &line, bibID, t, fieldC, v); err != nil {
					return nil, "", fmt.Errorf("parsing: %s", err)
				}
			default:
				return nil, "", fmt.Errorf("parsing: unknown data type in field \"" + t + "\"")
			}

		}
	}
	var instanceID string = getInstanceID(mrecs)
	if !isCurrent(state, instanceID) {
		return []Marc{}, "", nil
	}
	return mrecs, instanceID, nil
}

func transformSubfields(mrecs *[]Marc, line *int64, bibID string, field string, ord int64, sm map[string]interface{}) error {
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
				BibID:   bibID,
				Field:   field,
				Ind1:    ind1,
				Ind2:    ind2,
				Ord:     ord,
				SF:      k,
				Content: vs,
			})
			(*line)++
		}
	}

	return nil
}

func parseLeader(m map[string]interface{}) (string, error) {
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
		if rec.Field == "999" && rec.SF == "i" && rec.Content != "" {
			return rec.Content
		}
	}
	return ""
}

func isCurrent(state string, instanceID string) bool {
	return state == "ACTUAL" && instanceID != ""
}
