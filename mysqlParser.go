// A small parser for MySQL-Statements
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

	// AS converts the values to a AS-Statement (removes ';' if necessary)
	// values:	[0]:	select_query
	//		[1]:	table_name
	// result:	"(select_query) AS table_name;"
	AS = fillRequest(2, 2, fillAs)

	// AND converts the values to a AND-Statement
	// values:	[0]:	operand1
	//		[1]:	operand2
	// result:	"(operand1 AND operand2)"
	AND = fillRequest(2, 2, fillAnd)

	// OR converts the values to a Or-Statement
	// values:	[0]:	operand1
	//		[1]:	operand2
	// result:	"(operand1 OR operand2)"
	OR = fillRequest(2, 2, fillOr)

	// NOT converts the values to a NOT-Statement
	// values:	[0]:	operand
	// result:	"(NOT operand)"
	NOT = fillRequest(1, 1, fillNot)

	// REGEXP converts the values to a REGEXP-Statement
	// values:	[0]:	expression
	//		[1]:	pattern
	// result:	"(expression REGEXP pattern)"
	REGEXP = fillRequest(2, 2, fillRegexp)

	// SET converts the values to a SET-Statement
	// values:	[0]:	expression
	// result:	"SET expression;"
	SET = fillRequest(1, 1, fillSet)

	// UPDATE converts the values to a UPDATE-Statement
	// values:	[0]:	table
	//		[1]:	expression
	// result:	"UPDATE table SET expression;"
	UPDATE = fillRequest(2, 2, fillUpdate)
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

func fillAnd(values []string) (request string) {
	switch size := len(values); {
	case size == 2:
		request = "(" + values[0] + " AND " + values[1] + ")"
	}
	return
}

func fillOr(values []string) (request string) {
	switch size := len(values); {
	case size == 2:
		request = "(" + values[0] + " OR " + values[1] + ")"
	}
	return
}

func fillNot(values []string) (request string) {
	switch size := len(values); {
	case size == 1:
		request = "(NOT " + values[0] + ")"
	}
	return
}

func fillRegexp(values []string) (request string) {
	switch size := len(values); {
	case size == 2:
		request = "(" + values[0] + " REGEXP " + values[1] + ")"
	}
	return
}

func fillSet(values []string) (request string) {
	switch size := len(values); {
	case size == 1:
		request = "SET " + values[0] + ";"
	}
	return
}

func fillUpdate(values []string) (request string) {
	switch size := len(values); {
	case size == 2:
		request = "UPDATE " + values[0] + " SET " + values[1] + ";"
	}
	return
}
