package ydySqlParser

import (
	"fmt"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"strings"
)

func BuildNewSql(sql string) (string, error) {

	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		fmt.Println(err, sql)

		return sql, err
	}

	switch v := stmt.(type) {
	case *sqlparser.Select:

		new_subquery := &sqlparser.Subquery{Select: v}

		new_subquery = Subquery(new_subquery)

		//
		buf1 := sqlparser.NewTrackedBuffer(nil)
		new_subquery.Select.Format(buf1)

		return buf1.String(), nil
	}

	return sql, nil
}

func Subquery(v *sqlparser.Subquery) *sqlparser.Subquery {

	//range select field
	for i, vv := range v.Select.(*sqlparser.Select).SelectExprs {

		switch vvv := vv.(type) {
		case *sqlparser.StarExpr:

			//如果是 * 不再支持
			v.Select.(*sqlparser.Select).SelectExprs[i] = &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("invalid field *")), As: sqlparser.NewColIdent("")}

		case *sqlparser.AliasedExpr:
			//
			switch e := vvv.Expr.(type) {
			case *sqlparser.Subquery:

				e = Subquery(e)

			case *sqlparser.FuncExpr:
				//字段使用方法

				//如果不是insert 就要过滤下
				if strings.ToLower(e.Name.String()) != "insert" {
					// e = FuncExpr(e)
				}

				e = FuncExpr(e)

			case *sqlparser.ColName:

				// fmt.Printf("--val %#v \n", e.Name.String())

				//关键字段不能使用 As
				if keywordsFilter(e.Name.String()) && len(vvv.As.String()) > 0 {
					//
					//     // v.Select.(*sqlparser.Select).SelectExprs[i] = &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("invalid field *")), As: sqlparser.NewColIdent("")}
					//
					vvv.As = sqlparser.NewColIdent("")

				} else {
					e = ColName(e)
				}

			default:

				fmt.Printf("--val %#v \n", e)

			}
		}
	}

	//range from sql
	for _, vv := range v.Select.(*sqlparser.Select).From {

		switch vvv := vv.(type) {
		case *sqlparser.AliasedTableExpr:

			// fmt.Printf("vvv %#v", vvv)

			switch e := vvv.Expr.(type) {
			case *sqlparser.Subquery:

				e = Subquery(e)
			//
			// case *sqlparser.FuncExpr:
			//
			//     e = FuncExpr(e)
			//
			// case *sqlparser.ColName:
			//
			//     e = ColName(e)
			//
			default:
				// fmt.Printf("--val %#v \n", e)
			}

			// buf2 := sqlparser.NewTrackedBuffer(nil)
			// vvv.Expr.Format(buf2)

			//
			// source_table := buf2.String()
			//
			// target_table, _ := sqlparser.Parse("select * from test")
			//
			// new_select := target_table.(*sqlparser.Select)
			//
			// new_subquery := &sqlparser.Subquery{Select: new_select}
			//
			// vvv.Expr = new_subquery
		}
	}

	return v
}

func FuncExpr(e *sqlparser.FuncExpr) *sqlparser.FuncExpr {

	// fun_name := strings.ToLower(e.Name.String())
	//
	// //禁止字段使用以下函数
	// if t := func(str string) bool {
	//     funlist := []string{"left", "right", "elt", "replace", "insert", "substring", "CONCAT", "BIN", "oct", "hex", "ASCII"}
	//     for _, f := range funlist {
	//         if f == fun_name {
	//             return true
	//         }
	//     }
	//     return false
	// }(fun_name); t {
	//
	// }

	for i, ee := range e.Exprs {

		// fmt.Printf("ee : %#v \n", ee)

		switch eee := ee.(type) {
		case *sqlparser.AliasedExpr:

			switch eeee := eee.Expr.(type) {
			case *sqlparser.Subquery:

				eeee = Subquery(eeee)

			case *sqlparser.FuncExpr:

				eeee = FuncExpr(eeee)

			case *sqlparser.ColName:

				eeee = ColName(eeee)

				// fmt.Printf("ee, %s, %s \n", e.Name.CompliantName(), eeee.Name.String())

				//禁止字段使用 函数
				if keywordsFilter(eeee.Name.String()) {

					e.Exprs[i] = &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte(eeee.Name.String() + " field not use func")), As: sqlparser.NewColIdent("")}

				}

			default:

			}
			// case *sqlparser.JoinTableExpr:
		}
	}

	return e
}

func ColName(c *sqlparser.ColName) *sqlparser.ColName {

	// fmt.Printf("===> %#v ,\n", c.Name)

	// if c.Name.String() == "abc" {
	//
	// 	_colident := sqlparser.NewColIdent("md5(abcd)")
	//
	// 	c.Name = _colident
	// }

	return c
}

func keywordsFilter(str string) bool {

	fieldList := []string{
		"mobile",
		"u_mobile",
		"b_mobile",
		"bu_mobile",
		"link_mobile",
		"link2_mobile",
		"emergency_mobile",
		"link2_mate_mobile",
		"customer_verification",
		//
		"id_card",
		"b_id_card",
		"link_id_card",
		"link2_id_card",
		"link2_mate_id_card",
		//
		"bank_card_one",
		"bank_card_two",
		"bank_card",
		"b_bank_card",
	}
	for _, _f := range fieldList {
		if str == _f {
			return true
		}
	}
	return false
}
