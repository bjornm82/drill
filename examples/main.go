package main

import (
	"log"

	"github.com/bjornm82/drill"
	"github.com/pkg/errors"
)

const (
	host   = "localhost"
	port   = 8047
	useSSL = false
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

func main() {
	cl := drill.NewClient(host, port, useSSL)

	create := drill.Drill{}
	err := create.Unmarshal([]byte(testView))
	if err != nil {
		log.Fatalln(errors.Wrap(err, "unable to unmarshal view"))
	}
	resp, err := cl.UpsertView(create, "s3", "tmp")
	if err != nil {
		log.Fatalln(errors.Wrap(err, "unable to create view"))
	}
	// Should print "COMPLETED"
	log.Println(resp.QueryState)

	delete := drill.Drill{}
	delete.Name = "bank-data-func-test"
	resp, err = cl.DeleteView(delete, "s3", "tmp")
	if err != nil {
		log.Fatalln(errors.Wrap(err, "something is wrong when deleting the view"))
	}

	// Should print "COMPLETED"
	log.Println(resp.QueryState)
}
