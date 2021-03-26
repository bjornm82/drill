package drill

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var sqlQuery = "SELECT CAST(`id` AS VARCHAR(20)) AS `id`, CAST(`age` AS BIGINT) AS `age`, CAST(`sex` AS VARCHAR(20)) AS `sex`, CAST(`region` AS VARCHAR(20)) AS `region`, CAST(`income` AS DOUBLE) AS `income`, CAST(`married` AS VARCHAR(20)) AS `married`, CAST(`children` AS BIGINT) AS `children`, CAST(`car` AS VARCHAR(20)) AS `car`, CAST(`save_act` AS VARCHAR(20)) AS `save_act`, CAST(`current_act` AS VARCHAR(20)) AS `current_act`, CAST(`mortgage` AS VARCHAR(20)) AS `mortgage`, CAST(`pep` AS VARCHAR(20)) AS `pep` FROM `s3.default`.`bank-data-with-headers.csvh`"

var testView = `{
	"name": "bank-data-func-test",
	"sql": "` + sqlQuery + `",
	"fields": [
	   {
		"name": "id",
		"type": "VARCHAR",
		"precision": 20,
		"isNullable": true
	  },
	  {
		"name": "age",
		"type": "BIGINT",
		"isNullable": true
	  },
	  {
		"name": "income",
		"type": "DOUBLE",
		"isNullable": true
	  }
	]
  }`

func TestValidation(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	err := d.Unmarshal([]byte(testView))
	if err != nil {
		t.Error(err.Error())
	}

	ok, err := cl.Validate(d)
	assert.True(t, ok)
	assert.NoError(t, err)
}
func TestValidation_FailedNoView(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	ok, err := cl.Validate(d)
	assert.False(t, ok)
	assert.Error(t, err)
}
func TestValidation_FailedInCorrectSQL(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	err := d.Unmarshal([]byte(testView))
	if err != nil {
		t.Error(err.Error())
	}
	d.Sql = "`SELECT * FROM nononon`"
	ok, err := cl.Validate(d)
	assert.False(t, ok)
	assert.Error(t, err)
}

func TestUpsert(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	err := d.Unmarshal([]byte(testView))
	if err != nil {
		t.Error(t, err)
	}

	respBody, err := cl.UpsertView(d, "s3", "tmp")
	if err != nil {
		t.Error(t, err)
	}

	assert.Equal(t, "COMPLETED", respBody.QueryState)
}

func TestUpsert_FailedNoName(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	err := d.Unmarshal([]byte(testView))
	if err != nil {
		t.Error(t, err)
	}
	d.Name = ""

	respBody, err := cl.UpsertView(d, "s3", "tmp")
	assert.Error(t, err)
	assert.Equal(t, ResponseBody{}, respBody)
}

func TestUpsert_FailedNoSql(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	err := d.Unmarshal([]byte(testView))
	if err != nil {
		t.Error(t, err)
	}
	d.Sql = ""

	respBody, err := cl.UpsertView(d, "s3", "tmp")
	assert.Error(t, err)
	assert.Equal(t, ResponseBody{}, respBody)
}

func TestValidateSQL(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	query := sqlQuery
	resp, err := cl.ValidateSQL(query)
	if err != nil {
		t.Error(t, err)
	}

	assert.Equal(t, "COMPLETED", resp.QueryState)
}

func TestDelete(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	err := d.Unmarshal([]byte(testView))
	if err != nil {
		t.Error(t, err)
	}

	resp, err := cl.DeleteView(d, "s3", "tmp")
	if err != nil {
		t.Error(t, err)
	}

	assert.Equal(t, "COMPLETED", resp.QueryState)
}

func TestDelete_FailedNoName(t *testing.T) {
	cl := NewClient("localhost", 8047, false)
	d := Drill{}
	resp, err := cl.DeleteView(d, "s3", "tmp")
	assert.Error(t, err)
	assert.Equal(t, ResponseBody{}, resp)
}

func TestUnmarshal(t *testing.T) {
	d := Drill{}
	bytes := []byte("{")
	err := d.Unmarshal(bytes)
	assert.Error(t, err)
	assert.NotEmpty(t, fmt.Sprint(err))
}
