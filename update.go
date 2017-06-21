package ydySqlParser

import (
	"fmt"
	"github.com/youtube/vitess/go/vt/sqlparser"
	// "strings"
)

func BuildNewUpdateSql(sql, pkey string) (string, error) {

	//
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		fmt.Println(err, sql)

		return sql, err
	}

	var newSelect sqlparser.Select

	//
	switch v := stmt.(type) {
	case *sqlparser.Update:

		var newSelectExprs sqlparser.SelectExprs = make(sqlparser.SelectExprs, len(v.Exprs)+1)

		for i, vv := range v.Exprs {
			newSelectExprs[i] = &sqlparser.AliasedExpr{Expr: sqlparser.NewValArg([]byte(vv.Name.Name.String()))}
		}
		newSelectExprs[len(v.Exprs)] = &sqlparser.AliasedExpr{Expr: sqlparser.NewValArg([]byte(pkey))}

		//
		newSelect.SelectExprs = newSelectExprs
		newSelect.From = v.TableExprs
		newSelect.Where = v.Where
		newSelect.OrderBy = v.OrderBy
		newSelect.Limit = v.Limit
	}

	//

	buf1 := sqlparser.NewTrackedBuffer(nil)
	newSelect.Format(buf1)

	fmt.Println(buf1.String())

	return buf1.String(), nil
}

//func BuildNewBackUpdateSql(table_name string, rows []map[string]string, pKey string) (string, error) {
func BuildNewBackUpdateSql(table_name string, row map[string]string, pKey string) (string, error) {

	var newUpdate sqlparser.Update

	newUpdate.TableExprs = append(newUpdate.TableExprs, &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent(table_name), Qualifier: sqlparser.NewTableIdent("")}})

	// for _, v := range rows {
	for kk, vv := range row {

		if kk == pKey {
			newUpdate.Where = &sqlparser.Where{Type: "where", Expr: &sqlparser.ComparisonExpr{Operator: "=", Left: &sqlparser.ColName{Name: sqlparser.NewColIdent(pKey)}, Right: sqlparser.NewStrVal([]byte(vv))}}
		}

		_update := sqlparser.UpdateExpr{Name: &sqlparser.ColName{Name: sqlparser.NewColIdent(kk)}, Expr: sqlparser.NewStrVal([]byte(vv))}

		newUpdate.Exprs = append(newUpdate.Exprs, &_update)
	}
	// }

	//
	buf1 := sqlparser.NewTrackedBuffer(nil)
	newUpdate.Format(buf1)

	return buf1.String(), nil
}

func GetUpdateTableName(sql string) (string, []string) {

	tableName := ""
	fieldList := []string{}

	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		fmt.Println(err, sql)

		return tableName, fieldList
	}

	//
	switch v := stmt.(type) {
	case *sqlparser.Update:

		for _, vv := range v.Exprs {
			fieldList = append(fieldList, vv.Name.Name.String())
		}

		buf1 := sqlparser.NewTrackedBuffer(nil)
		v.TableExprs.Format(buf1)

		tableName = buf1.String()

	}

	return tableName, fieldList
}
