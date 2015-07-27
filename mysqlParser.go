package mysql-parser

import (
	"strings"
)

var (
	OPEN         = fillRequest(5, 5, fillOpen)
	SELECT       = fillRequest(2, 3, fillSelect)
	CREATE_TABLE = fillRequest(3, -1, fillCreate_Table)
	DROP_TABLE   = fillRequest(1, -1, fillDrop_Table)
	INSERT_INTO  = fillRequest(3, -1, fillInsert_Into)
	DELETE       = fillRequest(2, 2, fillDelete)
	AS           = fillRequest(2, 2, fillAs)
)

type fill func(values []string) (request string)

func fillRequest(min_args, max_args int, requestFiller fill) func([]string) string {
	return func(values []string) string {
		if (len(values) <= max_args || max_args < 0) && len(values) >= min_args {
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

func fillCreate_Table(values []string) (request string) {
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

func fillDrop_Table(values []string) (request string) {
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

func fillInsert_Into(values []string) (request string) {
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
