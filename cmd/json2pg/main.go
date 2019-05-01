package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

var (
	pgUser       = flag.String("U", "root", "Postgres user")
	pgPassword   = flag.String("P", "", "Postgres password")
	pgHost       = flag.String("h", "localhost", "Postgres host")
	pgPort       = flag.Uint("p", 5432, "Postgres port")
	databaseName = flag.String("d", "", "Database name")
	tableName    = flag.String("t", "", "Table name")
	fileName     = flag.String("f", "", "Input file name")
	ignoreErrors = flag.Bool("ignore-errors", false, "Ignore insert errors")
)

func main() {
	flag.Parse()
	if *databaseName == "" {
		flag.Usage()
		log.Fatal("Please specify database name")
	}
	if *tableName == "" {
		flag.Usage()
		log.Fatal("Please specify table name")
	}
	if *fileName == "" {
		flag.Usage()
		log.Fatal("Please specify input file name")
	}

	pg, err := pgx.Connect(pgx.ConnConfig{
		Host:                 *pgHost,
		User:                 *pgUser,
		Password:             *pgPassword,
		Port:                 uint16(*pgPort),
		Database:             *databaseName,
		PreferSimpleProtocol: true,
	})
	if err != nil {
		log.Fatalf("Failed to connect to db: %v", err)
	}
	defer pg.Close()

	file, err := os.Open(*fileName)
	if err != nil {
		log.Fatalf("Failed to open input file for reading: %v", err)
	}
	defer file.Close()
	var inputData []map[string]interface{}
	err = json.NewDecoder(file).Decode(&inputData)
	if err != nil {
		log.Fatalf("Failed to decode input data: %v", err)
	}
	if len(inputData) == 0 {
		log.Fatal("No rows in the input file")
	}

	cols, err := columns(pg, *databaseName, *tableName)
	if err != nil {
		log.Fatalf("Failed to read table structure: %v", err)
	}

	errors := make([]error, 0)
	var totalInserted int64
	for rowID, row := range inputData {
		var valuePlaceholders string
		fields := make([]string, 0, len(row))
		vals := make([]interface{}, 0, len(row))
		var i int
		for k, v := range row {
			if _, ok := cols[k]; !ok {
				continue
			}
			i++
			if i > 1 {
				valuePlaceholders += ","
			}
			valuePlaceholders += "$" + strconv.Itoa(i)
			fields = append(fields, `"`+k+`"`)

			if v != nil {
				switch {
				// handle number -> timestamp
				case reflect.TypeOf(v).Kind() == reflect.Float64 && strings.Contains(cols[k], "timestamp"):
					v = time.Unix(int64(v.(float64)), 0)
				// handle json/jsonb
				case reflect.TypeOf(v).Kind() == reflect.Map:
					b := bytes.NewBuffer(nil)
					err = json.NewEncoder(b).Encode(v)
					if err != nil {
						e := fmt.Errorf("Failed to encode json field %s: %v\n", k, err)
						if !*ignoreErrors {
							log.Fatal(e.Error())
						}
						errors = append(errors, e)
					}
					v = b.String()
				}
			}
			vals = append(vals, v)
		}
		q := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, *tableName, strings.Join(fields, ","), valuePlaceholders)
		ct, err := pg.Exec(q, vals...)
		if err != nil {
			e := fmt.Errorf("Failed to insert row #%d: %v\n\nquery: %s\n\nvals: %+v\n", rowID, err, q, vals)
			if !*ignoreErrors {
				log.Fatal(e.Error())
			}
			errors = append(errors, e)
		}
		totalInserted += ct.RowsAffected()
	}
	fmt.Printf("Inserted %d rows into %s\n", totalInserted, *tableName)
	if len(errors) > 0 {
		fmt.Printf("Errors occured during execution (%d):\n", len(errors))
		for i, err := range errors {
			fmt.Printf("#%d\n%s\n", i, err)
		}
		os.Exit(1)
	}
}

func columns(pg *pgx.Conn, dbName, tableName string) (map[string]string, error) {
	rows, err := pg.Query(
		`SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1 AND table_catalog=$2`,
		tableName, dbName,
	)
	if err != nil {
		return nil, errors.Wrap(err, "query failed")
	}
	defer rows.Close()
	cols := make(map[string]string)
	for rows.Next() {
		var n, t string
		err = rows.Scan(&n, &t)
		if err != nil {
			return nil, errors.Wrap(err, "scan failed")
		}
		cols[n] = t
	}
	return cols, nil
}
