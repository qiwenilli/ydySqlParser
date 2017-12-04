package ydySqlParser

import (
	"fmt"
	"github.com/youtube/vitess/go/vt/sqlparser"
	// "strings"
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
	for _, vv := range v.Select.(*sqlparser.Select).SelectExprs {

		switch vvv := vv.(type) {
		case *sqlparser.StarExpr:

			//如果是 * 不再支持
			// v.Select.(*sqlparser.Select).SelectExprs[i] = &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("invalid field *")), As: sqlparser.NewColIdent("")}

		case *sqlparser.AliasedExpr:
			//
			switch e := vvv.Expr.(type) {
			case *sqlparser.Subquery:

				e = Subquery(e)

			case *sqlparser.FuncExpr:

				e = FuncExpr(e)

			case *sqlparser.BinaryExpr:

				e = BinaryExpr(e)

			case *sqlparser.CaseExpr:

				e = CaseExpr(e)

			case *sqlparser.ParenExpr:

				e = ParenExpr(e)

			case *sqlparser.ColName:

				//关键字段不能使用 As
				if KeywordsFilter(e.Name.String(), "all") && len(vvv.As.String()) > 0 {
					//
					//     // v.Select.(*sqlparser.Select).SelectExprs[i] = &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("invalid field *")), As: sqlparser.NewColIdent("")}
					//
					vvv.As = sqlparser.NewColIdent("")

				} else {
					// e = ColName(e)
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

			}

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

	for _, ee := range e.Exprs {

		switch eee := ee.(type) {
		case *sqlparser.AliasedExpr:

			switch eeee := eee.Expr.(type) {
			case *sqlparser.Subquery:

				eeee = Subquery(eeee)

			case *sqlparser.FuncExpr:

				eeee = FuncExpr(eeee)

			case *sqlparser.ColName:

				eeee = ColName(eeee)

				// //禁止字段使用 函数
				// if KeywordsFilter(eeee.Name.String(), "all") {
				//
				// 	e.Exprs[i] = &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte(eeee.Name.String() + " field not use func")), As: sqlparser.NewColIdent("")}
				//
				// }

			}
			// case *sqlparser.JoinTableExpr:
		}
	}

	return e
}

func BinaryExpr(b *sqlparser.BinaryExpr) *sqlparser.BinaryExpr {

	switch ee := b.Left.(type) {
	case *sqlparser.BinaryExpr:

		ee = BinaryExpr(ee)

	case *sqlparser.ColName:

		ee = ColName(ee)

	}

	switch ee := b.Right.(type) {
	case *sqlparser.BinaryExpr:

		ee = BinaryExpr(ee)

	case *sqlparser.ColName:

		ee = ColName(ee)

	}

	return b
}

func CaseExpr(e *sqlparser.CaseExpr) *sqlparser.CaseExpr {

	for _, w := range e.Whens {
		switch ww := w.Val.(type) {
		case *sqlparser.Subquery:

			ww = Subquery(ww)

		case *sqlparser.FuncExpr:

			ww = FuncExpr(ww)

		case *sqlparser.BinaryExpr:

			ww = BinaryExpr(ww)

		case *sqlparser.ParenExpr:

			ww = ParenExpr(ww)

		case *sqlparser.ColName:

			ww = ColName(ww)

		}
	}

	return e
}

func ParenExpr(e *sqlparser.ParenExpr) *sqlparser.ParenExpr {

	switch ee := e.Expr.(type) {
	case *sqlparser.BinaryExpr:

		ee = BinaryExpr(ee)

	}

	return e
}

func ColName(c *sqlparser.ColName) *sqlparser.ColName {

	if KeywordsFilter(c.Name.CompliantName(), "all") {

		// return &sqlparser.NewStrVal([]byte("invalid field -"))

		c.Name = sqlparser.NewColIdent(c.Name.CompliantName() + " Don't support syntax")

	}

	return c
}

func KeywordsFilter(str string, field_type string) bool {

	mobile_fieldList := []string{
		"mobile",
		"u_mobile",
		"b_mobile",
		"bu_mobile",
		"link_mobile",
		"link2_mobile",
		"emergency_mobile",
		"link2_mate_mobile",
		"customer_verification",
		"bd_tel",
		"c_mobile",
		"master_mobile",
		"slave_mobile",
		"phone",
		"ho_phone",
		"customer_mobile",
		"pt_mobile",
		"mobile_bak",
	}
	idcard_fieldList := []string{
		//
		"id_card",
		"b_id_card",
		"link_id_card",
		"link2_id_card",
		"link2_mate_id_card",
	}
	bankcard_fieldList := []string{
		//
		"bank_card_one",
		"bank_card_two",
		"bank_card",
		"b_bank_card",
	}

	var fieldList []string

	switch field_type {
	case "mobile":
		fieldList = mobile_fieldList
	case "idcard":
		fieldList = idcard_fieldList
	case "bankcard":
		fieldList = bankcard_fieldList
	case "all":
		fieldList = append(fieldList, mobile_fieldList...)
		fieldList = append(fieldList, idcard_fieldList...)
		fieldList = append(fieldList, bankcard_fieldList...)
	}

	for _, _f := range fieldList {
		if str == _f {
			return true
		}
	}
	return false
}
