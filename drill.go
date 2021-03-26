package drill

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/xeipuuv/gojsonschema"
)

const (
	drillSchemaPath = "file://./schema.json"

	dropViewFmt   = "DROP VIEW `%s`.`%s`.`%s`"
	createViewFmt = "CREATE OR REPLACE VIEW `%s`.`%s`.`%s` AS "
)

type Drill struct {
	Name   string  `json:"name"`
	Sql    string  `json:"sql"`
	Fields []Field `json:"fields"`
}

type Field struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Precision  int    `json:"precision"`
	IsNullable bool   `json:"isNullable"`
}

func (d *Drill) Validate() (bool, error) {
	ls := gojsonschema.NewReferenceLoader(drillSchemaPath)
	ld := gojsonschema.NewGoLoader(d)

	result, err := gojsonschema.Validate(ls, ld)
	if err != nil {
		return false, errors.Wrap(err, "unable to validate schema")
	}

	if !result.Valid() {
		fmt.Printf("The document is not valid. see errors :\n")
		for _, desc := range result.Errors() {
			fmt.Printf("- %s\n", desc)
		}

		return false, errors.New("document is not valid")
	}

	// TODO: Not validating, should be probably with Drill by just querying the API
	// _, err = sqlparser.Parse(d.Sql)
	// if err != nil {
	// 	return false, errors.Wrap(err, "not able to parse query")
	// }

	return true, nil
}

func (c *Client) UpsertView(d Drill, source, workspace string) (ResponseBody, error) {
	var path = "query.json"
	ok, err := d.Validate()
	if !ok {
		return ResponseBody{}, errors.New("object not through validation")
	}
	if err != nil {
		return ResponseBody{}, errors.Wrap(err, "object not valid")
	}

	if d.Name == "" {
		return ResponseBody{}, errors.New("upsert view needs a name")
	}

	if d.Sql == "" {
		return ResponseBody{}, errors.New("upsert view needs an sql statement")
	}

	base := fmt.Sprintf(createViewFmt, source, workspace, d.Name)

	u := RequestBody{
		QueryType: "SQL",
		Query:     base + d.Sql,
	}

	return c.post(path, u)
}

func (c *Client) DeleteView(d Drill, source, workspace string) (ResponseBody, error) {
	var path = "query.json"
	if d.Name == "" {
		return ResponseBody{}, errors.New("delete view needs a name")
	}

	query := fmt.Sprintf(dropViewFmt, source, workspace, d.Name)
	u := RequestBody{
		QueryType: "SQL",
		Query:     query,
	}

	return c.post(path, u)
}

func (c *Client) ValidateSQL(sql string) (ResponseBody, error) {
	var path = "query.json"

	u := RequestBody{
		QueryType: "SQL",
		Query:     sql,
	}

	return c.post(path, u)
}

func (d *Drill) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, d)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal data to drill object")
	}
	return nil
}

func (d *Drill) Marshal(data []byte) ([]byte, error) {
	return json.Marshal(d)
}
