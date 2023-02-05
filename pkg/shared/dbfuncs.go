package shared

import (
	"database/sql"
	"fmt"
	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
	"github.com/grammaton76/g76golib/pkg/sjson"
	"github.com/lib/pq"
	"os"
	"reflect"
	"strings"
	"time"
)

type TxBlock struct {
	Label        string
	Stmt         *sql.Stmt
	IgnoreAllErr bool
	Args         []interface{}
}

type DbType int

const (
	DbTypeUndef    DbType = 0
	DbTypeMysql    DbType = 1
	DbTypePostgres DbType = 2
)

type Stmt struct {
	*sql.Stmt
	dbh      *DbHandle
	sql      string
	argorder []int
	failure  error
}

type DbHandle struct {
	*sql.DB
	Name      string
	Section   string // Section that the config settings came from
	Key       string // Config key pointing to the section (i.e. scraper.dbhandle=scrapedb; that points at the section)
	dbtype    DbType
	Host      string
	DbName    string
	Username  string
	Password  string
	ReadOnly  bool
	warnings  []error
	failed    error
	prepCache map[string]*sql.Stmt
}

func (sth *Stmt) Err() error {
	return sth.failure
}

func (sth *Stmt) Identify() string {
	return fmt.Sprintf("(query sql '%s' for %s)", sth.sql, sth.dbh.DbName)
}

func (dbh *DbHandle) PrepareOrDie(sql string) *Stmt {
	return dbh.Prepare(sql).OrDie()
}

func (dbh *DbHandle) Postmortem() []error {
	var DumpPacket string
	var Dying bool
	var Errors []error
	if dbh == nil {
		Errors = append(Errors, fmt.Errorf("nil db handle"))
		return Errors
	}
	if len(dbh.warnings) != 0 {
		DumpPacket += fmt.Sprintf("non-fatal warnings: %v", dbh.warnings)
	}
	if dbh.failed != nil {
		Dying = true
		Errors = append(Errors, fmt.Errorf("fatal errors: %v\n", dbh.failed))
	}
	if dbh.DB == nil {
		Errors = append(Errors, fmt.Errorf("nil sql.DB object"))
		return Errors
	}
	err := dbh.DB.Ping()
	if err != nil {
		Errors = append(Errors, fmt.Errorf("failed ping: %s.\n%s\n", err, DumpPacket))
		return Errors
	}
	if Dying {
		return Errors
	}
	if DumpPacket != "" {
		log.Debugf("No fatal errors on '%s', but non-fatal errors follow:\n%s\n", dbh.Identifier(), DumpPacket)
	}
	return nil
}

func (dbh *DbHandle) IsDead() bool {
	return dbh.Postmortem() != nil
}

func (dbh *DbHandle) OrDie() *DbHandle {
	Errors := dbh.Postmortem()
	if Errors == nil {
		return dbh
	}
	log.Fatalf("Fatal errors on '%s':\n%+v", dbh.Identifier(), Errors)
	return dbh
}

func (dbh *DbHandle) Identifier() string {
	if dbh.Key != "" {
		return fmt.Sprintf("db defined in section '%s' (sourced from key '%s')", dbh.Section, dbh.Key)
	}
	if dbh.Section != "" {
		return fmt.Sprintf("dbh defined in section '%s'", dbh.Section)
	}
	return "unidentifiable db handle"
}

func (dbh *DbHandle) Connect() error {
	var err error
	switch dbh.dbtype {
	case DbTypeMysql:
		err = dbh.connectDbMysql()
	case DbTypePostgres:
		err = dbh.connectDbPg()
	default:
		log.Fatalf("Attempted to call connect on '%s' when we had no db type - '%s'!\n", dbh.Identifier(), dbh.failed)
	}
	return err
}

func (dbh *DbHandle) connectDbMysql() error {
	var Dsn string
	if dbh.DbName != "" {
		Dsn = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?parseTime=true&charset=utf8mb4_general_ci,utf8&loc=%s",
			dbh.Username, dbh.Password, dbh.Host, dbh.DbName, time.Local.String())
	} else {
		Dsn = fmt.Sprintf("%s:%s@tcp(%s:3306)/?parseTime=true&charset=utf8mb4_general_ci,utf8&loc=%s",
			dbh.Username, dbh.Password, dbh.Host, time.Local.String())
	}
	// fmt.Printf("Host: '%s', database: '%s', user: '%s'\n", DbHost, DbName, DbUser)
	var err error
	dbh.DB, err = sql.Open("mysql", Dsn)
	dbh.dbtype = DbTypeMysql
	if err != nil {
		dbh.failed = err
		return fmt.Errorf("DB: %+v\n Error: %+v\n\n", dbh.Identifier(), err)
	}
	return dbh.failed
}

func (dbh *DbHandle) connectDbPg() error {
	var Dsn string
	if dbh.Username != "" {
		Dsn += fmt.Sprintf("user=%s ", dbh.Username)
	}
	if dbh.DbName != "" {
		Dsn += fmt.Sprintf("dbname=%s ", dbh.DbName)
	}
	Dsn += fmt.Sprintf("sslmode=disable ")
	if dbh.Password != "" {
		Dsn += fmt.Sprintf("password=%s ", dbh.Password)
	}
	if dbh.Host != "" {
		Dsn += fmt.Sprintf("host=%s ", dbh.Host)
	}
	var err error
	dbh.DB, err = sql.Open("postgres", Dsn)
	dbh.dbtype = DbTypePostgres
	if err != nil {
		dbh.failed = err
		return err
	}
	log.Debugf("Made db connection as '%s' to '%s.%s'\n", dbh.Username, dbh.Host, dbh.DbName)
	return dbh.failed
}

func RunAndGetLastInsertId(stmt *Stmt, Options ...interface{}) (int64, error) {
	switch stmt.dbh.dbtype {
	case DbTypeMysql:
		res, err := stmt.Exec(Options...)
		if err != nil {
			log.Fatalf("ERROR on chat.chatMessageInsert: %s\n", err)
		}
		row, err := res.LastInsertId()
		return row, err
	case DbTypePostgres:
		var row *int64
		var err error
		err = stmt.QueryRow(Options...).Scan(&row)
		if err != nil {
			log.Fatalf("In chat.chatMessageInsert: %s (or id scan thereof): %s\n", stmt.Identify(), err)
		}
		if row == nil {
			log.Errorf("Null result back from QueryRow, but no error set; unclear if the database worked or not.\n")
		}
		return *row, nil
	}
	return 0, fmt.Errorf("unknown database type on %s", stmt.dbh.Identifier())
}

func InsertJsonAsDbRow(Table string, Data *sjson.JSON, Db *sql.DB) error {
	var Cols []string
	var Placeholders []string
	var Vals []interface{}
	for k, v := range *Data {
		Cols = append(Cols, k)
		Placeholders = append(Placeholders, "?")
		Vals = append(Vals, v)
	}
	Query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", Table,
		strings.Join(Cols, ","),
		strings.Join(Placeholders, ","),
	)
	Stmt, err := Db.Prepare(Query)
	log.FatalIff(err, "InsertJsonAsDbRow failed prepare on table '%s' with query '%s'", Table, Query)
	_, err = Stmt.Exec(Vals...)
	Stmt.Close()
	log.ErrorIff(err, "InsertJsonAsDbRow failed exec on table '%s' with query '%s'", Table, Query)
	return nil
}

var transToPsql sjson.JSON
var transToMysql sjson.JSON

func init() {
	transToMysql = sjson.NewJson()
	transToMysql["NOW"] = "NOW()"
	transToPsql = sjson.NewJson()
	transToPsql["NOW"] = "'now'"
}

func (dbh *DbHandle) Translate(sql string) string {
	var Translated string
	switch dbh.dbtype {
	case DbTypeMysql:
		Translated = transToMysql.TemplateString(sql)
	case DbTypePostgres:
		Translated = transToPsql.TemplateString(sql)
	}
	return Translated
}

func (dbh *DbHandle) TransPrep(sql string) *Stmt {
	if dbh == nil {
		log.Fatalf("ERROR: TransPrep for '%s' called with a nil database handle!\n", sql)
	}
	return dbh.Prepare(dbh.Translate(sql))
}

func (dbh *DbHandle) Prepare(sql string) *Stmt {
	if dbh == nil {
		log.Fatalf("ERROR: Prepare for '%s' called with a nil database handle!\n", sql)
	}
	stmt, err := dbh.DB.Prepare(sql)
	log.Debugf("DB '%s': Prepare'ing query '%s'\n", dbh.Identifier(), sql)
	/*	if dbh.dbtype==DbTypeMysql {
		var KeepGoing=true
		var Arg int = 1
		for KeepGoing {
			KeepGoing=false
			// Need to check for $\d+ and replace it as appropriate.
			// Also need to support use case of same column appearing multiple times.
		}
	}*/
	var Bob Stmt
	Bob.dbh = dbh
	Bob.Stmt = stmt
	Bob.sql = sql
	Bob.failure = err
	return &Bob
}

func (sth *Stmt) OrDie(msgs ...string) *Stmt {
	log.FatalIff(sth.failure, "Failed to prepare statement: %s\nSQL: %s\n%s\n",
		sth.dbh.Identifier(), sth.sql, strings.Join(msgs, "\n"))
	return sth
}

func (sth *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	Argc := len(sth.argorder)
	if Argc > 0 {
		var nargs []interface{}
		for _, v := range sth.argorder {
			nargs = append(nargs, args[v])
		}
		return sth.Stmt.Exec(nargs)
	}
	return sth.Stmt.Exec(args...)
}

func PrepareOrDie(dbh *DbHandle, sql string) *Stmt {
	if dbh == nil {
		log.Fatalf("Prepare for '%s' called with a nil database handle!\n", sql)
	}
	Caw := dbh.Prepare(sql).OrDie()
	return Caw
}

func (dbh *DbHandle) ErrorType(err error) string {
	var besterror string
	if err == nil {
		return ""
	}
	switch dbh.dbtype {
	case DbTypeMysql:
		besterror = "err_mysql_unknown"
		if mysqlError, ok := err.(*mysql.MySQLError); ok {
			besterror = fmt.Sprintf("mysql_errno_%d", mysqlError)
			log.Debugf("mysql error number %d\n", mysqlError)
			if mysqlError.Number == mysqlerr.ER_DUP_ENTRY {
				return "duplicate_key"
			}
		} else {
			log.Printf("We received an error of type '%T'\n", err)
		}
	case DbTypePostgres:
		besterror = "err_pgsql_unknown"
		if pgError, ok := err.(*pq.Error); ok {
			Code := pgError.Code.Name()
			log.Debugf("pq error: %s (class %s)\n", Code, pgError.Code.Class())
			switch Code {
			case "unique_violation":
				return "duplicate_key"
			default:
				return "err_pgsqlundef_" + Code
			}
		}
	default:
		return "err_unknown_db"
	}
	return besterror
}

func (dbh *DbHandle) DbType() DbType {
	return dbh.dbtype
}

func GetDbType(db *DbHandle) string {
	Type := reflect.ValueOf(db.Driver()).Type().String()
	switch Type {
	case "*pq.Driver":
		return "pgsql"
	case "*mysql.MySQLDriver":
		return "mysql"
	default:
		log.Printf("Unknown database type '%s'\n", Type)
		return "unknown"
	}
}

func ValidatePreparedQueriesOrDie(Q interface{}) {
	var validator sjson.JSON
	var NilStatements bool
	validator = sjson.NewJson()
	validator.IngestFromObject(Q)
	for k, v := range validator {
		Type := reflect.ValueOf(v).Type().String()
		switch Type {
		case "*shared.Stmt":
			if validator[k] == nil {
				log.Critf("Prepared query %s is NIL\n", k)
				NilStatements = true
			}
		case "string":
			log.Debugf("String key '%s' has value '%s'\n", k, validator[k].(string))
		default:
			log.Warnf("Unknown type '%s'; can't validate.\n", Type)
		}
	}
	if NilStatements {
		log.Printf("Exiting due to undefined prepared statement slots (see above)\n")
		os.Exit(1)
	}
}

func FormatMysqlTime(Time time.Time) string {
	return Time.Format("2006-01-02 15:04:05")
}

func ApplyTxBlock(Db *sql.DB, Blocks []TxBlock) error {
	//log.Printf("Started transaction block with %d entries.\n", len(Blocks))
	tx, err := Db.Begin()
	if err != nil {
		log.Critf("ERROR on startup of transaction: %s!\n", err)
		return err
	}
	for k, v := range Blocks {
		//log.Printf("Performing statement %d in transaction block ('%s')\n", k+1, v.Label)
		tStmt := tx.Stmt(v.Stmt)
		_, err := tStmt.Exec(v.Args...)
		if err != nil && !v.IgnoreAllErr {
			log.Critf("ERROR executing transaction %d ('%s') in block: %s\n", k+1, v.Label, err)
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		log.Critf("ERROR on final commit after transaction %d in block: %s!\n", len(Blocks)+1, err)
		return err
	}
	return nil
}

func ParseMysqlTime(k string) (*time.Time, error) {
	if k == "" {
		return nil, nil
	}
	localLoc, err := time.LoadLocation("Local")
	if err != nil {
		log.Fatalf(`Failed to load location "Local": %s`, err)
	}
	var Time time.Time
	if strings.Index(k, "Z") != -1 {
		Time, err = time.ParseInLocation("2006-01-02T15:04:05Z", k, localLoc)
	} else {
		Time, err = time.ParseInLocation("2006-01-02T15:04:05-07:00", k, localLoc)
	}
	if err != nil {
		return nil, err
	}
	localDateTime := Time.Local()
	//	log.Printf("CAW\nInput: %s\nOutput: %s\n", k, localDateTime.String())
	//	os.Exit(1)
	return &localDateTime, nil
}

func DeferCloseDb(db *sql.DB, label string) {
	log.ErrorIff(db.Close(), "Closing database handle '%s'", label)
}
