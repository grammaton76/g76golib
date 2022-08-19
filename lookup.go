package g76golib

import (
	"database/sql"
	"fmt"
	"os"
)

/*
CREATE TABLE _LOOKUP_ (id serial not null primary key, name varchar(20) unique);
*/

type LookupItem interface {
	Name() string
	Id() int
	Table() LookupTable
	IsNil() bool
}

type lookupMember struct {
	parent *lookupTable
	name   string
	id     int
	notnil bool
}

func (i lookupMember) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{ "id": %d, "name": "%s" }`,
		i.id, i.name)), nil
}

type LookupTable interface {
	ByIdOrDie(int) LookupItem
	ById(int) LookupItem
	LabelToId(string, bool) LookupItem
	ByNameOrAdd(string) LookupItem
	ByName(string) LookupItem
}

type lookupTable struct {
	db              *DbHandle
	TableName       string
	LoadQuery       string
	SelectNameQuery string
	SelectIdQuery   string
	InsertQuery     string
	insertStmt      *Stmt
	selectNameStmt  *Stmt
	selectIdStmt    *Stmt
	loadStmt        *Stmt
	compiled        bool
	labelToId       map[string]*lookupMember
	idToLabel       map[int]*lookupMember
}

func (lm lookupMember) Name() string {
	return lm.name
}

func (lm lookupMember) Id() int {
	return lm.id
}

func (lm lookupMember) Table() LookupTable {
	return lm.parent
}

func (lm *lookupMember) IsNil() bool {
	if lm == nil {
		return true
	}
	return !lm.notnil
}

func (l *lookupTable) ByNameOrAdd(label string) LookupItem {
	if l.labelToId[label] == nil {
		l.LabelToId(label, true)
	}
	return l.labelToId[label]
}

func (l *lookupTable) ByName(label string) LookupItem {
	return l.labelToId[label]
}

func (l *lookupTable) ByNameOrDie(label string) LookupItem {
	if l.compiled == false {
		l.Compile()
	}
	return l.labelToId[label]
}

func (l *lookupTable) ByIdOrDie(id int) LookupItem {
	if l.compiled == false {
		l.Compile()
	}
	Lookup, found := l.idToLabel[id]
	if !found {
		log.Fatalf("Tried to resolve unknown market id '%d'\n", id)
	}
	return Lookup
}

func (l *lookupTable) ById(id int) LookupItem {
	if l.compiled == false {
		l.Compile()
	}
	Lookup, found := l.idToLabel[id]
	if !found {
		return nil
	}
	return Lookup
}

func (l *lookupTable) LabelToId(label string, create bool) LookupItem {
	var err error
	var labelid = l.labelToId[label]
	//fmt.Printf("Looking up label '%s'\n", label)
	if labelid != nil {
		return labelid
	}
	//log.Printf("Query '%s', label '%s'\n", l.SelectIdQuery, label)
	selDb := l.selectIdStmt.QueryRow(label)
	var id int
	err = selDb.Scan(&id)
	if err == sql.ErrNoRows {
		//fmt.Printf("No rows returned.\n")
		if create == true {
			Rec := l.insertLabel(label)
			return Rec
		}
		return nil
	}
	log.FatalIff(err, "LoadLookup LabelToId scan (%s): %s\n", label, err)
	return nil
}

func (l *lookupTable) Compile() {
	if l.compiled == true {
		fmt.Printf("Dangerous sketchy land: We're re-compiling a lookup!\n")
	}
	if l.db == nil {
		fmt.Printf("Attempting to compile lookuptable %s with nil db handle!\n", l.TableName)
	}
	l.loadStmt = l.db.Prepare(l.LoadQuery)
	if l.loadStmt.Err() != nil {
		log.Fatalf("Compile() for load query on table '%s' on handle '%s': %s\n", l.TableName, l.db.Identifier(), l.loadStmt.Err())
		os.Exit(1)
	}
	l.insertStmt = l.db.Prepare(l.InsertQuery)
	if l.insertStmt.Err() != nil {
		log.Fatalf("Compile() for insert on table %s on handle '%s': %s\n", l.TableName, l.db.Identifier(), l.insertStmt.Err())
		os.Exit(1)
	}
	l.selectIdStmt = l.db.Prepare(l.SelectIdQuery)
	if l.selectIdStmt.Err() != nil {
		log.Fatalf("Compile() for select id on table %s on handle '%s': %s\n", l.TableName, l.db.Identifier(), l.selectIdStmt.Err())
		os.Exit(1)
	}
	l.compiled = true
}

func (l *lookupTable) CompileIfNeeded() {
	if l.compiled == false {
		l.Compile()
	}
}

func NewLookup(tablename string, Db *DbHandle) LookupTable {
	var LT lookupTable
	LT.db = Db
	LT.TableName = tablename
	LT.LoadQuery = fmt.Sprintf("SELECT id,name FROM %s;", tablename)
	switch GetDbType(Db) {
	case "mysql":
		LT.SelectNameQuery = fmt.Sprintf("SELECT name FROM %s WHERE id=?;", tablename)
		LT.SelectIdQuery = fmt.Sprintf("SELECT id FROM %s WHERE name=?;", tablename)
		LT.InsertQuery = fmt.Sprintf("INSERT INTO %s (name) VALUES (?);", tablename)
	case "pgsql":
		LT.SelectNameQuery = fmt.Sprintf("SELECT name FROM %s WHERE id=$1;", tablename)
		LT.SelectIdQuery = fmt.Sprintf("SELECT id FROM %s WHERE name=$1;", tablename)
		LT.InsertQuery = fmt.Sprintf("INSERT INTO %s (name) VALUES ($1) RETURNING id;", tablename)
	}
	LT.compiled = false
	LT.labelToId = make(map[string]*lookupMember)
	LT.idToLabel = make(map[int]*lookupMember)
	LT.LoadLookup()
	return &LT
}

func (l *lookupTable) insertLabel(label string) LookupItem {
	Rec := lookupMember{
		name:   label,
		parent: l,
		notnil: true,
	}
	if l.db.DbType() == DbTypePostgres {
		Row := l.insertStmt.QueryRow(label)
		var id int
		err := Row.Scan(&id)
		log.FatalIff(err, "Can't insert '%s' into '%s'\n", label, l.TableName)
		Rec.id = id
	} else {
		res, err := l.insertStmt.Exec(label)
		log.FatalIff(err, "Failed on insertLabel for '%s'\n", l.TableName)
		var id int64
		id, err = res.LastInsertId()
		log.FatalIff(err, "Failed to get id on insertLabel for '%s'\n", l.TableName)
		Rec.id = int(id)
	}
	l.labelToId[label] = &Rec
	l.idToLabel[Rec.id] = &Rec
	return &Rec
}

func (l *lookupTable) LoadLookup() *lookupTable {
	var id int
	var label string
	l.CompileIfNeeded()
	selDB, err := l.loadStmt.Query()
	if err != nil {
		fmt.Printf("ERROR on LoadLookup: %s\n", err)
		os.Exit(1)
	}
	for selDB.Next() {
		err = selDB.Scan(&id, &label)
		if err != nil {
			panic(err.Error())
		}
		Rec := lookupMember{
			parent: l,
			id:     id,
			name:   label,
			notnil: true,
		}
		l.labelToId[label] = &Rec
		l.idToLabel[id] = &Rec
	}
	return l
}
