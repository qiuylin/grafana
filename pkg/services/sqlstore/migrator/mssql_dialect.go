package migrator

import (
	"fmt"
	"strconv"
	"strings"

	mssql "github.com/denisenkom/go-mssqldb"
	"xorm.io/xorm"
)

type MSSQLDialect struct {
	BaseDialect
}

func NewMssqlDialect(engine *xorm.Engine) Dialect {
	d := MSSQLDialect{}
	d.BaseDialect.dialect = &d
	d.BaseDialect.engine = engine
	d.BaseDialect.driverName = MSSQL
	return &d
}

func (db *MSSQLDialect) SupportEngine() bool {
	return false
}

func (db *MSSQLDialect) Quote(name string) string {
	return "\"" + name + "\""
}

func (db *MSSQLDialect) AutoIncrStr() string {
	return "IDENTITY"
}

func (db *MSSQLDialect) BooleanStr(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func (db *MSSQLDialect) SQLType(c *Column) string {
	var res string
	switch c.Type {
	case DB_MediumInt, DB_Integer:
		c.Length = 0
		res = DB_Int
	case DB_TinyText, DB_MediumText, DB_LongText, DB_Text:
		res = DB_Varchar + "(MAX)"
	case DB_Bool:
		res = DB_Bit
	case DB_Blob, DB_TinyBlob, DB_MediumBlob, DB_LongBlob:
		res = DB_VarBinary + "(MAX)"
	case DB_Double:
		res = DB_Float
	default:
		res = c.Type
	}

	var hasLen1 = (c.Length > 0)
	var hasLen2 = (c.Length2 > 0)

	if hasLen2 {
		res += "(" + strconv.Itoa(c.Length) + "," + strconv.Itoa(c.Length2) + ")"
	} else if hasLen1 {
		res += "(" + strconv.Itoa(c.Length) + ")"
	}

	return res
}

func (db *MSSQLDialect) CreateTableSQL(table *Table) string {
	sql := "IF NOT EXISTS (SELECT * FROM sysobjects WHERE NAME='" + table.Name + "' and xtype='U')\n"

	sql += "CREATE TABLE "
	sql += db.dialect.Quote(table.Name) + " (\n"

	pkList := table.PrimaryKeys

	for _, col := range table.Columns {
		if col.IsPrimaryKey && len(pkList) == 1 {
			sql += col.String(db.dialect)
		} else {
			sql += col.StringNoPk(db.dialect)
		}
		sql = strings.TrimSpace(sql)
		sql += "\n, "
	}

	if len(pkList) > 1 {
		quotedCols := []string{}
		for _, col := range pkList {
			quotedCols = append(quotedCols, db.dialect.Quote(col))
		}

		sql += "PRIMARY KEY ( " + strings.Join(quotedCols, ",") + " ), "
	}

	sql = sql[:len(sql)-2] + ");\n"

	return sql
}

func (db *MSSQLDialect) CopyTableData(sourceTable string, targetTable string, sourceCols []string, targetCols []string) string {
	sourceColsSQL := db.QuoteColList(sourceCols)
	targetColsSQL := db.QuoteColList(targetCols)

	quote := db.dialect.Quote
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;\n", quote(targetTable), targetColsSQL, sourceColsSQL, quote(sourceTable))
	identityInsertSQL := fmt.Sprintf("SET IDENTITY_INSERT %s ON;\n%s\nSET IDENTITY_INSERT %s OFF;", quote(targetTable), insertSQL, quote(targetTable))

	return fmt.Sprintf(
		"BEGIN TRY\n"+
			"%s\n"+
		"END TRY\n"+
		"BEGIN CATCH\n"+
			"SELECT ERROR_NUMBER() AS [Error Number], ERROR_MESSAGE() AS [ErrorMessage];\n"+
			"IF ERROR_NUMBER() IN (544) -- Cannot insert explicit value for identity column when IDENTITY_INSERT is set to OFF.\n"+
			"BEGIN\n"+
				"%s\n"+
			"END\n"+
			"ELSE\n"+
			"BEGIN\n"+
				"THROW -- Throw an exception from the TRY transaction\n"+
			"END\n"+
		"END CATCH", insertSQL, identityInsertSQL)
}

func (db *MSSQLDialect) DropTable(tableName string) string {
	return fmt.Sprintf("IF EXISTS (SELECT * FROM sysobjects WHERE id = "+
		"object_id(N'%s') and OBJECTPROPERTY(id, N'IsUserTable') = 1) "+
		"DROP TABLE \"%s\"", tableName, tableName)
}

func (db *MSSQLDialect) RenameTable(oldName string, newName string) string {
	quote := db.dialect.Quote
	return fmt.Sprintf("EXEC sp_rename %s, %s", quote(oldName), quote(newName))
}

func (db *MSSQLDialect) UpdateTableSQL(tableName string, columns []*Column) string {
	var statements = []string{}

	for _, col := range columns {
		statements = append(statements, "ALTER TABLE "+db.Quote(tableName)+" ALTER COLUMN "+col.StringNoPk(db))
	}

	return strings.Join(statements, ";\n")
}

func (db *MSSQLDialect) AddColumnSQL(tableName string, col *Column) string {
	return fmt.Sprintf("ALTER TABLE %s ADD %s", db.dialect.Quote(tableName), col.StringNoPk(db.dialect))
}

func (db *MSSQLDialect) IndexCheckSQL(tableName, indexName string) (string, []interface{}) {
	args := []interface{}{tableName, indexName}
	sql := "SELECT 1 FROM sys.indexes WHERE object_id = (SELECT object_id FROM sys.objects WHERE name=?) and name=?"
	return sql, args
}

func (db *MSSQLDialect) ColumnCheckSQL(tableName, columnName string) (string, []interface{}) {
	args := []interface{}{tableName, columnName}
	sql := "SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(?) AND NAME = ?"
	return sql, args
}

func (db *MSSQLDialect) CreateIndexSQL(tableName string, index *Index) string {
	quote := db.dialect.Quote
	var unique string
	if index.Type == UniqueIndex {
		unique = " UNIQUE"
	}

	idxName := index.XName(tableName)

	quotedCols := []string{}
	for _, col := range index.Cols {
		quotedCols = append(quotedCols, db.dialect.Quote(col))
	}

	whereNotNull := []string{}
	for _, col := range index.Cols {
		whereNotNull = append(whereNotNull, db.dialect.Quote(col)+" IS NOT NULL ")

	}

	return fmt.Sprintf("CREATE%s INDEX %v ON %v (%v) WHERE %v;", unique, quote(idxName), quote(tableName), strings.Join(quotedCols, ","), strings.Join(whereNotNull, "AND "))
}

func (db *MSSQLDialect) CleanDB() error {
	tables, _ := db.engine.DBMetas()
	sess := db.engine.NewSession()
	defer sess.Close()

	for _, table := range tables {
		if _, err := sess.Exec("ALTER TABLE " + table.Name + " NOCHECK CONSTRAINT ALL;"); err != nil {
			return fmt.Errorf("failed to disable constraint checks, err: %v", err)
		}
		if _, err := sess.Exec("DROP TABLE IF EXISTS " + table.Name + " ;"); err != nil {
			return fmt.Errorf("failed to delete table: %v, err: %v", table.Name, err)
		}
	}

	return nil
}

func (db *MSSQLDialect) Limit(limit int64) string {
	return fmt.Sprintf(" OFFSET 0 ROWS FETCH NEXT %d ROWS ONLY", limit)
}

func (db *MSSQLDialect) LimitOffset(limit int64, offset int64) string {
	return fmt.Sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit)
}

func (db *MSSQLDialect) isThisError(err error, errcode int32) bool {
	if driverErr, ok := err.(*mssql.Error); ok {
		if driverErr.SQLErrorNumber() == errcode {
			return true
		}
	}

	return false
}

func (db *MSSQLDialect) IsUniqueConstraintViolation(err error) bool {
	return db.isThisError(err, 2627) || db.isThisError(err, 2601)
}

func (db *MSSQLDialect) ErrorMessage(err error) string {
	if driverErr, ok := err.(*mssql.Error); ok {
		return driverErr.Message
	}

	return ""
}

func (db *MSSQLDialect) IsDeadlock(err error) bool {
	return db.isThisError(err, 1205)
}