package mysqlParser

import (
	"strings"
)

var (
	// OPEN converts the values to the data source for the Go-MySQL-Driver to be
	// used as argument for sql.Open
	// values:	[0]:	username
	//		[1]:	password
	//		[2]:	IP
	//		[3]:	port
	//		[4]:	db_name
	// result:	"username:password@tcp(IP:port)/db_name"
	OPEN = fillRequest(5, 5, fillOpen)

	// SELECT converts the values to a basic SELECT-Statement
	// values:	[0]:	select_expr
	//		[1]:	table_reference
	//		[2]:	where_condition	(optional)
	// result:	"SELECT select_expr FROM table_reference;"
	//		"SELECT select_expr FROM table_reference WHERE where_condition;"
	SELECT = fillRequest(2, 3, fillSelect)

	// CREATE_TABLE converts the values to a CREATE TABLE-Statement
	// values:	[0]:		table_name
	//		[1 ... x]:	columns
	//		[x + 1]:	engine_name
	// result:	"CREATE TABLE IF NOT EXISTS table_name(column1, column2, ..., columnX) ENGINE= engine_name;"
	CREATE_TABLE = fillRequest(3, -1, fillCreateTable)

	// DROP_TABLE converts the values to a DROP TABLE-Statement
	// values:	[0 ... x]:	tables
	// result:	"DROP TABLE IF EXISTS table1, table2, ..., tableX;"
	DROP_TABLE = fillRequest(1, -1, fillDropTable)

	// INSERT_INTO converts the values to a INSERT INTO-Statement
	// values:	[0]:		table_name
	//		[1]:		colunms
	//		[2]:		values1
	//		[3 ... X]:	values2 ... valuesX	(optional)
	// result:	"INSERT INTO table_name(columns) VALUES(values1), (values2), ..., (valuesX);"
	INSERT_INTO = fillRequest(3, -1, fillInsertInto)

	// DELETE converts the values to a DELETE-Statement
	// values:	[0]:	table_name
	//		[1]:	where_condition
	// result:	"DELETE FROM table_name WHERE where_condition;"
	DELETE = fillRequest(2, 2, fillDelete)

	// AS converts the values to basic a AS-Statement (removes ';' if necessary)
	// values:	[0]:	select_query
	//		[1]:	table_name
	// result:	(select_query) AS table_name;"
	AS = fillRequest(2, 2, fillAs)
)

type fill func(values []string) (request string)

func fillRequest(minArgs, maxArgs int, requestFiller fill) func([]string) string {
	return func(values []string) string {
		if (len(values) <= maxArgs || maxArgs < 0) && len(values) >= minArgs {
			return requestFiller(values)
		}
		return ""
	}
}

func fillOpen(values []string) (request string) {
	return values[0] + ":" + values[1] + "@tcp(" + values[2] + ":" + values[3] + ")/" + values[4]
}

func fillSelect(values []string) (request string) {
	switch len(values) {
	case 3:
		request = request + " WHERE " + values[2]
		fallthrough
	case 2:
		request = "SELECT " + values[0] + " FROM " + values[1] + request + ";"
	}
	return
}

func fillCreateTable(values []string) (request string) {
	switch size := len(values); {
	case size > 3:
		for i := 2; i < size-1; i++ {
			request += ", " + values[i]
		}
		fallthrough
	case size == 3:
		request = "CREATE TABLE IF NOT EXISTS " + values[0] + "(" + values[1] + request + ") ENGINE=" + values[size-1] + ";"
	}
	return
}

func fillDropTable(values []string) (request string) {
	switch size := len(values); {
	case size > 1:
		for i := 1; i < size; i++ {
			request += ", " + values[i]
		}
		fallthrough
	case size == 1:
		request = "DROP TABLE IF EXISTS " + values[0] + request + ";"
	}
	return
}

func fillInsertInto(values []string) (request string) {
	switch size := len(values); {
	case size > 3:
		for i := 3; i < size; i++ {
			request += ", (" + values[i] + ")"
		}
		fallthrough
	case size == 3:
		request = "INSERT INTO " + values[0] + "(" + values[1] + ") VALUES(" + values[2] + ")" + request + ";"
	}
	return
}

func fillDelete(values []string) (request string) {
	switch size := len(values); {
	case size == 2:
		request = "DELETE FROM " + values[0] + " WHERE " + values[1] + ";"
	}
	return
}

func fillAs(values []string) (request string) {
	switch size := len(values); {
	case size == 2:
		request = "(" + strings.TrimSuffix(values[0], ";") + ") AS " + values[1] + ";"
	}
	return
}
