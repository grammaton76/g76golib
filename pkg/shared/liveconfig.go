package shared

import (
	"database/sql"
	"fmt"
	"github.com/grammaton76/g76golib/pkg/sjson"
	"reflect"
	"time"
)

type LiveConfig struct {
	db  *DbHandle
	dbq struct {
		CheckLiveConfig  *Stmt
		UpdateLiveConfig *Stmt
		GetLiveConfig    *Stmt
	}
	Pulse   int
	content map[string]*LiveConfigKey
}

type LiveConfigKey struct {
	Parent  *LiveConfig
	Label   string
	Addr    interface{}
	Type    string
	Version int
	Updated string
}

func NewLiveConfig() *LiveConfig {
	Lc := &LiveConfig{}
	Lc.content = make(map[string]*LiveConfigKey)
	return Lc
}

func (Lck *LiveConfigKey) String() string {
	switch Lck.Type {
	case "*string":
		return *(Lck.Addr.(*string))
	default:
		return fmt.Sprintf("String() cast from '%s' is undefined.\n",
			Lck.Type)
	}
}

func (Lck *LiveConfigKey) Replicate() error {
	DbVal := Lck.String()
	_, err := Lck.Parent.dbq.UpdateLiveConfig.Exec(Lck.Label, DbVal)
	return err
}

func (Lc *LiveConfig) KeyList() sjson.JSONarray {
	Res, err := Lc.dbq.CheckLiveConfig.Query()
	if err != nil {
		log.Errorf("DB failure pulling live config for API: %s\n", err)
		return nil
	}
	Json := sjson.NewJsonArray()
	Json.ScanRows(Res)
	return Json
}

func (Lc *LiveConfig) BindDb(db *DbHandle) *LiveConfig {
	Lc.db = db
	Lc.dbq.GetLiveConfig = db.PrepareOrDie(
		`SELECT content FROM liveconfig WHERE label=$1;`)
	Lc.dbq.CheckLiveConfig = db.PrepareOrDie(
		`SELECT label, updated, content FROM liveconfig;`)
	Lc.dbq.UpdateLiveConfig = db.PrepareOrDie(
		`UPDATE liveconfig SET content=$2, updated=NOW() WHERE label=$1;`)
	return Lc
}

func (Lc *LiveConfig) FinishLoad() {
	// TODO: Change this to a channel as it's more go-like
	for Lc.Pulse < 1 {
		time.Sleep(time.Second)
	}
	log.Printf("Initial load of liveconfig done.\n")
}

func (Lc *LiveConfig) WatchConfigs(delay time.Duration) {
	var Pulses int
	for true {
		log.Debugf("Config loader thread %d starting\n", Pulses)
		Dv, err := Lc.dbq.CheckLiveConfig.Query()
		if err != nil {
			log.Fatalf("Failed to pull data versions: %s\n", err)
		}
		for Dv.Next() {
			var (
				Label   string
				Updated string
				Content sql.NullString
			)
			err = Dv.Scan(&Label, &Updated, &Content)
			if err != nil {
				log.Errorf("Failed to scan liveconfig: %s\n", err)
				continue
			}
			Lck := Lc.KeyRef(Label)
			if Lck == nil {
				continue
			}
			if Lck.Updated != Updated {
				Res := Lc.dbq.GetLiveConfig.QueryRow(Label)
				var Value string
				err = Res.Scan(&Value)
				if err != nil {
					log.Errorf("DB error loading liveconfig string value '%s': %s\n", Label, err)
					continue
				}
				log.Printf("%s cached is %s, db is %s\n",
					Label, Lck.Updated, Updated)
				Lck.Update(Content.String)
				Lck.Updated = Updated
			}
		}
		Lc.Pulse++
		time.Sleep(delay)
	}
}

func (Lc *LiveConfig) Update(Label string, Value interface{}) *LiveConfigKey {
	Lck, found := Lc.content[Label]
	if !found {
		log.Errorf("Couldn't find key '%s' in LiveConfig.\n", Label)
	}
	return Lck.Update(Value)
}

func (Lck *LiveConfigKey) Update(Value interface{}) *LiveConfigKey {
	vType := reflect.TypeOf(Value).String()
	switch vType {
	case "*string":
		Lck.updateString(*Value.(*string))
	case "string":
		Lck.updateString(Value.(string))
	default:
		log.Errorf("Unable to update LiveConfig key label '%s' (type %s) to value '%s'\n",
			Lck.Label, Lck.Type, Value)
		return nil
	}
	return Lck
}
func (Lck *LiveConfigKey) updateString(Value string) *LiveConfigKey {
	switch Lck.Type {
	case "*string":
		log.Printf("Update LiveConfigKey '%s' from value '%s' to value '%s'\n", Lck.Label,
			*Lck.Addr.(*string), Value)
		*(Lck.Addr.(*string)) = Value
	default:
		log.Errorf("Unable to update LiveConfig key label '%s' (type %s) to value '%s'\n",
			Lck.Label, Lck.Type, Value)
		return nil
	}
	Lck.Version++
	Lck.Updated = time.Now().String()
	return Lck
}

func (Lc *LiveConfig) KeyRef(Label string) *LiveConfigKey {
	Lck, found := Lc.content[Label]
	if !found {
		log.Errorf("Couldn't find key '%s' in LiveConfig.\n", Label)
	}
	return Lck
}

func (Lc *LiveConfig) Bind(Label string, Addr interface{}) *LiveConfigKey {
	Type := reflect.TypeOf(Addr).String()
	switch Type {
	case "*string":
	default:
		log.Errorf("No idea how to bind live config key '%s' to type '%s'\n",
			Label, Type)
		return nil
	}
	Lck := LiveConfigKey{
		Parent: Lc,
		Label:  Label,
		Type:   Type,
		Addr:   Addr,
	}
	log.Debugf("Successfully bound live config key '%s' to a value of type '%s'\n",
		Label, Type)
	Lc.content[Label] = &Lck
	return &Lck
}
