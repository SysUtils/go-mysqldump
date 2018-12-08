package mysqldump

import (
	"database/sql"
	"errors"
	"strings"
	"text/template"
	"time"
	"fmt"
	"bytes"
)

type table struct {
	Name   string
	SQL    string
	Values []string
}

type dump struct {
	DumpVersion   string
	ServerVersion string
	DBName        string
	DBSQL         string
	Tables        []*table
	CompleteTime  string
}

const version = "0.2.2"


func getTemplate() (string) {
	rawtemplate := `-- Go SQL Dump {{ .DumpVersion }}
--
-- ------------------------------------------------------
-- Server version	{{ .ServerVersion }}

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


DROP DATABASE IF EXISTS {{ .DBName }};

{{ .DBSQL }};

USE {{ .DBName }};

{{range .Tables}}
--
-- Table structure for table {{ .Name }}
--

DROP TABLE IF EXISTS {{ .Name }};
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
{{ .SQL }};
/*!40101 SET character_set_client = @saved_cs_client */;
--
-- Dumping data for table {{ .Name }}
--

LOCK TABLES {{ .Name }} WRITE;
/*!40000 ALTER TABLE {{ .Name }} DISABLE KEYS */;
{{$name := .Name}}
{{range .Values}}{{ if . }}INSERT INTO {{ $name }} VALUES {{ . }};
{{ end }}{{ end }}

/*!40000 ALTER TABLE {{ .Name }} ENABLE KEYS */;
UNLOCK TABLES;
{{ end }}
-- Dump completed on {{ .CompleteTime }}`
	rawtemplate = strings.Replace(rawtemplate, "{{ .DBName }}", "`{{ .DBName }}`", -1)
	rawtemplate = strings.Replace(rawtemplate, "{{ $name }}", "`{{ $name }}`", -1)
	return strings.Replace(rawtemplate, "{{ .Name }}", "`{{ .Name }}`", -1)
}

// Creates a MYSQL Dump based on the options supplied through the dumper.
func Dump(db *sql.DB, dbname string) (string, error) {
	p := ""
	var err error


	// Get server version
	serverVersion, err := getServerVersion(db)
	if err != nil {
		return p, err
	}

	// Get sql for create DB
	dbsql, err := createDatabaseSQL(db, dbname)
	if err != nil {
		return p, err
	}

	// Get tables
	tables, err := getTables(db)
	if err != nil {
		return p, err
	}

	// Get sql for each table
	tablelist := make([]*table,0)
	for _, name := range tables {
		if t, err := createTable(db, name); err == nil {
			tablelist = append(tablelist, t)
		} else {
			return p, err
		}
	}

	// Set complete time
	data := dump{
		DumpVersion:  version,
		Tables:       tablelist,
		DBName:       dbname,
		DBSQL:        dbsql,
		CompleteTime: time.Now().String(),
		ServerVersion:serverVersion,
	}

	// Write dump to buffer
	var tpl bytes.Buffer
	t, err := template.New("mysqldump").Parse(getTemplate())
	if err != nil {
		return p, err
	}
	if err = t.Execute(&tpl, data); err != nil {
		return p, err
	}

	return tpl.String(), nil
}

func getTables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)

	// Get table list
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tables = append(tables, table.String)
	}
	return tables, rows.Err()
}

func getServerVersion(db *sql.DB) (string, error) {
	var server_version sql.NullString
	if err := db.QueryRow("SELECT version()").Scan(&server_version); err != nil {
		return "", err
	}
	return server_version.String, nil
}

func createTable(db *sql.DB, name string) (*table, error) {
	var err error
	t := &table{Name: name}

	if t.SQL, err = createTableSQL(db, name); err != nil {
		return nil, err
	}

	if t.Values, err = createTableValues(db, name); err != nil {
		return nil, err
	}

	return t, nil
}

func createDatabaseSQL(db *sql.DB, name string) (string, error) {
	// Get table creation SQL
	var database_return sql.NullString
	var database_sql sql.NullString
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE DATABASE `%s`", name)).Scan(&database_return, &database_sql)

	if err != nil {
		return "", err
	}
	if database_return.String != name {
		return "", errors.New("Returned database is not the same as requested table")
	}

	return database_sql.String, nil
}

func createTableSQL(db *sql.DB, name string) (string, error) {
	// Get table creation SQL
	var table_return sql.NullString
	var table_sql sql.NullString
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", name)).Scan(&table_return, &table_sql)

	if err != nil {
		return "", err
	}
	if table_return.String != name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return table_sql.String, nil
}


func createTableValues(db *sql.DB, name string) ([]string, error) {
	// Get Data
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get columns
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, errors.New("No columns in table " + name + ".")
	}

	// Read data
	data_text := make([]string, 0)

	for rows.Next() {
		// Init temp data storage

		//ptrs := make([]interface{}, len(columns))
		//var ptrs []interface {} = make([]*sql.NullString, len(columns))

		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i, _ := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = "'" + value.String + "'"
			} else {
				dataStrings[key] = "null"
			}
		}

		data_text = append(data_text, "("+strings.Join(dataStrings, ",")+")")
	}


	var batches []string
	batchSize := 1000
	for batchSize < len(data_text) {
		data_text, batches = data_text[batchSize:], append(batches, strings.Join(data_text[0:batchSize:batchSize], ","))
	}
	batches = append(batches, strings.Join(data_text, ","))

	return batches, rows.Err()
}
