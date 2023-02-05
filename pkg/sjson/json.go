package sjson

import (
	"bytes"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/grammaton76/g76golib/pkg/slogger"
	"github.com/shopspring/decimal"
	"golang.org/x/sys/unix"
	"math"
	"net/http"
	"os"
	"strings"
	"time"
)

var DefaultHeaders map[string]string

func init() {
	DefaultHeaders = make(map[string]string)
}

type JSON map[string]interface{}

type JSONarray []JSON

var log *slogger.Logger

func init() {
	log = &slogger.Logger{}
}

func SetLogger(l *slogger.Logger) *slogger.Logger {
	log = l
	return l
}

func (j *JSON) KeyBool(k string) bool {
	if (*j)[k] == nil {
		return false
	}
	return (*j)[k].(bool)
}

func (j *JSON) KeyJson(k string) *JSON {
	if j == nil {
		return nil
	}
	if (*j)[k] == nil {
		return nil
	}
	Caw := NewJson()
	Caw.IngestFromObject((*j)[k])
	return &Caw
}

func (j *JSON) KeyString(k string) string {
	if j == nil {
		return ""
	}
	if (*j)[k] == nil {
		return ""
	}
	var Ret string
	switch (*j)[k].(type) {
	case float64:
		Bob := (*j)[k].(float64)
		if Bob == math.Trunc(Bob) {
			Ret = fmt.Sprintf("%.0f", (*j)[k])
		} else {
			Ret = fmt.Sprintf("%f", (*j)[k])
		}
	case bool:
		Ret = fmt.Sprintf("%t", (*j)[k])
	default:
		Ret = (*j)[k].(string)
	}
	return Ret
}

func (j *JSON) KeyDecimal(k string) decimal.Decimal {
	if (*j)[k] == nil {
		return decimal.Zero
	}
	switch (*j)[k].(type) {
	case float64:
		return decimal.NewFromFloat((*j)[k].(float64))
	case string:
		Dec, err := decimal.NewFromString((*j)[k].(string))
		log.ErrorIff(err, "string '%s' at key '%s' not mappable to decimal.\n",
			(*j)[k], k)
		return Dec
	default:
		log.Errorf("key '%s' of object not mappable to decimal.\n", k)
	}
	return decimal.Zero
}

func (j *JSON) KeyFloat64(k string) float64 {
	if (*j)[k] == nil {
		return 0
	}
	return (*j)[k].(float64)
}

func (j *JSON) KeyInt(k string) int {
	if (*j)[k] == nil {
		return 0
	}
	return (*j)[k].(int)
}

func (j *JSON) KeyInt64(k string) int64 {
	if (*j)[k] == nil {
		return 0
	}
	return (*j)[k].(int64)
}

func (j *JSON) KeyIntFromFloat64(k string) int {
	if (*j)[k] == nil {
		return 0
	}
	//log.Printf("Attempting key '%s'\n", k)
	Caw := (*j)[k].(float64)
	return int(Caw)
}

func (j *JSON) KeyIntFrom64(k string) int {
	if (*j)[k] == nil {
		return 0
	}
	//log.Printf("Attempting key '%s'\n", k)
	Caw := (*j)[k].(int64)
	return int(Caw)
}

func (j *JSON) ScanRow(Result *sql.Rows) error {
	var Caw JSONarray
	err := Caw.ScanRows(Result)
	if err != nil {
		return nil
	}
	Length := len(Caw)
	if Length == 0 {
		return sql.ErrNoRows
	}
	if Length == 1 {
		*j = Caw[0]
		return nil
	}
	return fmt.Errorf("result set had %d rows; ScanRow function only works with one.", Length)
}

func (j *JSONarray) ScanRows(Result *sql.Rows) error {
	var ScanArray []interface{}
	var Buf JSON
	Buf.New()
	Types, err := Result.ColumnTypes()
	if err != nil {
		log.Fatalf("Fatal error pulling result metadata: %s!\n", err)
	}
	//log.Printf("Found types: %+v\n", Types)
	for i, v := range Types {
		var Target interface{}
		Name := v.Name()
		log.Debugf("Column %d ('%s') is a '%+v'\n", i+1, Name, v.ScanType())
		switch v.ScanType().String() {
		case "uint32":
			Caw := int32(0)
			Caw2 := &Caw
			Buf[Name] = &Caw2
		case "bool":
			Caw := bool(false)
			Caw2 := &Caw
			Buf[Name] = &Caw2
		case "uint64":
			Caw := int64(0)
			Caw2 := &Caw
			Buf[Name] = &Caw2
		case "int32":
			Caw := int(0)
			Buf[Name] = &Caw
		case "sql.NullInt64":
			Caw := int64(0)
			Caw2 := &Caw
			Buf[Name] = &Caw2
		case "sql.NullFloat64":
			Caw := float64(0)
			Caw2 := &Caw
			Buf[Name] = &Caw2
		case "sql.RawBytes", "mysql.NullTime", "sql.NullTime", "string":
			Caw := string("")
			Caw2 := &Caw
			Buf[Name] = &Caw2
		case "time.Time":
			Caw := time.Time{}
			Caw2 := &Caw
			Buf[Name] = &Caw2
		case "interface {}":
			Caw := string("")
			Caw2 := &Caw
			Buf[Name] = &Caw2
			log.Warnf("You should probably cast column %d to a type in your query.\n", i)
		default:
			log.Fatalf("Column %d: Haven't ever encountered type '%s' before, don't know how to handle it!\n", i, v.ScanType())
		}
		Target = Buf[Name]
		ScanArray = append(ScanArray, Target)
	}
	for Result.Next() {
		err = Result.Scan(ScanArray...)
		if err != nil {
			log.Fatalf("Fatal error scanning rows: '%s'!\n", err)
		}
		var S JSON
		S.New()
		for k, v := range ScanArray {
			//log.Printf("Column %d\n", k)
			if v != nil {
				Name := Types[k].Name()
				Type := Types[k].ScanType().String()
				switch Type {
				case "sql.NullFloat64":
					Caw := *(v.(**float64))
					if Caw == nil {
						S[Name] = nil
					} else {
						S[Name] = *Caw
					}
				case "int32":
					S[Name] = *(v.(*int))
				case "uint32":
					Caw := *(v.(**int32))
					if Caw == nil {
						S[Name] = nil
					} else {
						S[Name] = *Caw
					}
				case "sql.NullInt64", "uint64":
					Caw := *(v.(**int64))
					if Caw == nil {
						S[Name] = nil
					} else {
						S[Name] = *Caw
					}
				case "time.Time":
					Caw := *(v.(**time.Time))
					if Caw == nil {
						S[Name] = nil
					} else {
						S[Name] = *Caw
					}
				case "sql.RawBytes", "mysql.NullTime", "sql.NullTime", "string":
					Caw := *(v.(**string))
					if Caw == nil {
						S[Name] = nil
					} else {
						S[Name] = *Caw
					}
				case "bool":
					S[Name] = *(v.(**bool))
				default:
					log.Fatalf("Unknown type %s on scan!\n", Type)
				}
			} else {
				S[Types[k].Name()] = nil
			}
		}
		*j = append(*j, S)
		//log.Printf("We received the following: %+v\n", ScanArray)
		//log.Printf("Translated to: %+v\n", S)
	}
	return nil
}

func (j *JSON) AddFromForm(r *http.Request) {
	r.ParseForm()
	for k := range r.Form {
		v := r.FormValue(k)
		(*j)[k] = v
	}
	return
}

func LoadJsonFromFileOrDie(File string, Purpose string) JSON {
	dat, err := os.ReadFile(File)
	if err != nil {
		log.Critf("JSON file file '%s' didn't load!\n%s\n", File, Purpose)
		os.Exit(1)
	}
	var JsonStore JSON
	err = json.Unmarshal(dat, &JsonStore)
	if err != nil {
		log.FatalTracef("WARNING: %s had nonsensical JSON!\n%s\nError: %s\n", File, Purpose, err)
	}
	return JsonStore
}

func (j *JSON) SendJsonPost(Client *http.Client, url string) (*http.Response, error) {
	body, err := json.Marshal(*j)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	//log.Printf("POSTing JSON object to '%s'...\n", url)
	if Client == nil {
		http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		Client = &http.Client{
			Timeout: time.Second * 10,
		}
	}
	for k, v := range DefaultHeaders {
		//log.Printf("Setting header '%s' to '%s'\n", k, v)
		req.Header.Set(k, v)
	}
	resp, err := Client.Do(req)
	if err != nil {
		log.Printf("Error posting to '%s': %s\n", url, err)
	}
	return resp, err
}

func (j *JSON) SpiderCopyIniSectionFrom(Section *ini.Section) {
	//log.Printf("Started SpiderCopyIniSectionFrom on %s\n", Section.Name())
	for _, v := range Section.ChildSections() {
		Name := v.Name()
		var Bob *JSON
		if (*j)[Name] == nil {
			Bob.New()
			(*j)[Name] = Bob
		} else {
			log.Printf("We need to fuse %s (%T) but have no code for it  (usually this is a conflict between an ini section and vault secrets).\n", Name, Name)
		}
		Bob.SpiderCopyIniSectionFrom(v)
		(*j)[Name] = Bob
	}
	for _, v := range Section.Keys() {
		(*j)[v.Name()] = v.Value()
	}
}

func (j *JSON) SpiderCopyIniFrom(ini *ini.File) {
	if ini == nil {
		return
	}
	//log.Printf("Started SpiderCopyIniFrom (file)\n")
	for _, v := range ini.Sections() {
		Name := v.Name()
		var Bob JSON
		if (*j)[v.Name()] == nil {
			Bob.New()
		} else {
			var Caw interface{}
			Caw = (*j)[v.Name()]
			Bob.IngestFromObject(Caw)
			Bob.SpiderCopyIniSectionFrom(v)
			log.Debugf("We need to fuse %s (%T) but have no code for it (usually this is a conflict between an ini section and vault secrets).\n", Name, Name)
			//log.Debugf("Secret content was %s\n", Name)
		}
		Bob.SpiderCopyIniSectionFrom(v)
		(*j)[Name] = Bob
	}
}

func escapePerl(x string) string {
	var Quote = "'"
	//Start:=x[0:]
	//End := x[:0]
	if strings.Contains(x, "'") {
		Quote = "\""
		log.Debugf("#Protected you from a quote.\n")
	}
	return Quote + x + Quote
}

func (j *JSON) ExportAsPerlCode(Header string) string {
	var Buf string
	for k, v := range *j {
		switch v.(type) {
		case JSON:
			//log.Printf("%s is JSON.\n", k)
			SendHeader := Header
			if Header != "" {
				Header = Header + "."
			}
			Buf += v.(*JSON).ExportAsPerlCode(SendHeader + k)
		case string:
			Buf += fmt.Sprintf("$Config{'%s.%s'}=%s;\n", Header, k, escapePerl(v.(string)))
		default:
			log.Printf("Who knows what %s is?\n", k)
		}
	}
	return Buf
}

func escapePhp(x string) string {
	var Quote = "'"
	//Start:=x[0:]
	//End := x[:0]
	if strings.Contains(x, "'") {
		Quote = "\""
		log.Debugf("#Protected you from a quote.\n")
	}
	return Quote + x + Quote
}

func (j *JSON) ExportAsPhpCode(Header string) string {
	var Buf string
	if Header == "" {
		Buf = "$ini=array();\n"
	}
	for k, v := range *j {
		switch v.(type) {
		case JSON:
			SendHeader := Header
			//log.Printf("%s is JSON.\n", k)
			Buf += v.(*JSON).ExportAsPhpCode(SendHeader + "['" + k + "']")
		case string:
			Buf += fmt.Sprintf("$ini%s['%s']=%s;\n", Header, k, escapePhp(v.(string)))
		default:
			log.Printf("Who knows what %s is?\n", k)
		}
	}
	return Buf
}

func (j *JSON) ExportAsIniString() string {
	var Buf string
	for k, v := range *j {
		switch v.(type) {
		case JSON:
			//log.Printf("%s is JSON.\n", k)
			Buf += fmt.Sprintf("[%s]\n", k) + v.(*JSON).ExportAsIniString() + "\n"
		case string:
			Buf += fmt.Sprintf("%s=%s\n", k, v.(string))
		default:
			log.Printf("Who knows what %s is?\n", k)
		}
	}
	return Buf
}

func (j *JSON) SpiderCopyJsonFrom(Obj JSON) {
	//log.Printf("Entered SpiderCopyJsonFrom of %+v into %+v\n", Obj, Target)
	for k, v := range Obj {
		switch v.(type) {
		case JSON:
			//log.Printf("... key '%s' is a map. SpiderCopyJsonFrom Descending into further madness.\n", k)
			var Caw JSON
			if (*j)[k] != nil {
				Caw = (*j)[k].(JSON)
			} else {
				Caw.New()
			}
			Caw.SpiderCopyJsonFrom(v.(JSON))
			(*j)[k] = Caw
			//log.Printf("Spidered result is %s\n", Caw)
		default:
			//log.Printf("Who knows what %s.%s should do?\n", k, v)
			(*j)[k] = v.(string)
		}
	}
	//log.Printf("Finished SpiderCopyJsonFrom section; results are %v\n", *Target)
}

func checkCanWriteFile(Filename string) error {
	err := unix.Access(Filename, unix.O_RDWR|unix.O_CREAT)
	if errors.Is(err, unix.ENOENT) {
		f, err := os.Create(Filename)
		f.Close()
		if err == nil {
			os.Remove(Filename)
			log.Debugf("Confirmed '%s' doesn't exist, but may be created.\n", Filename)
			return nil
		}
	} else if err != nil {
		return fmt.Errorf("access check couldn't open temp file '%s', but it does exist: %s",
			Filename, err)
	}
	return nil
}

func (j *JSON) TestWritable(Filename string) error {
	err := checkCanWriteFile(Filename + ".tmp")
	if err != nil {
		return fmt.Errorf("couldn't open temp file '%s': %s", Filename+".tmp", err)
	}
	log.Debugf("Successfully tested write access to '%s'\n", Filename+".tmp")
	err = checkCanWriteFile(Filename)
	if err != nil {
		return fmt.Errorf("couldn't open real file '%s': %s", Filename, err)
	}
	return nil
}

func (j *JSON) WriteToFile(Filename string) error {
	f, err := os.Create(Filename + ".tmp")
	if err != nil {
		err = fmt.Errorf("sjson.WriteToFile(): couldn't create '%s' due to %s", Filename+".tmp", err)
		return err
	} else {
		_, err = f.Write(j.Bytes())
		if err != nil {
			err = fmt.Errorf("sjson.WriteToFile(): couldn't write '%s' due to %s", Filename+".tmp", err)
			return err
		}
		err = f.Close()
		if err != nil {
			err = fmt.Errorf("sjson.WriteToFile(): couldn't close '%s' due to %s", Filename+".tmp", err)
			return err
		}
	}
	err = os.Rename(Filename+".tmp", Filename)
	if err != nil {
		err = fmt.Errorf("sjson.WriteToFile(): couldn't rename '%s' as '%s' due to %s", Filename+".tmp", Filename, err)
		return err
	}
	return nil
}

func (j *JSON) ReadFromFile(Filename string) error {
	dat, err := os.ReadFile(Filename)
	if err != nil {
		log.Printf("ERROR! File '%s' didn't load: '%s'!\n", Filename, err)
		return err
	}
	return j.IngestFromBytes(dat)
}

func (j *JSON) IngestFromObject(Input interface{}) error {
	var Binary []byte
	Binary, _ = json.Marshal(Input)
	return json.Unmarshal(Binary, j)
}

func (j *JSONarray) IngestFromObject(Input interface{}) error {
	var Binary []byte
	Binary, _ = json.Marshal(Input)
	return json.Unmarshal(Binary, j)
}

func (j *JSONarray) IngestFromBytes(Input []byte) error {
	var err error
	err = json.Unmarshal(Input, &j)
	return err
}

func (j *JSON) IngestFromBytes(Input []byte) error {
	var err error
	err = json.Unmarshal(Input, j)
	return err
}

func (j *JSON) IngestFromString(Input string) error {
	return j.IngestFromBytes([]byte(Input))
}

func NewJsonFromObject(Input interface{}) *JSON {
	Binary, err := json.Marshal(Input)
	log.ErrorIff(err, "Marshal error in newjsonfromobject")
	var Caw JSON
	Caw.IngestFromBytes(Binary)
	return &Caw
}

func NewJsonFromObjectPtr(Input *interface{}) *JSON {
	Binary, err := json.Marshal(Input)
	log.ErrorIff(err, "Marshal error in newjsonfromobject")
	var Caw JSON
	Caw.IngestFromBytes(Binary)
	return &Caw
}

func NewJsonFromString(Input string) JSON {
	var Caw JSON
	Caw.New()
	Caw.IngestFromString(Input)
	return Caw
}

func (j *JSON) New() *JSON {
	*j = make(JSON)
	return j
}

func NewJson() JSON {
	var NewObj JSON
	NewObj = make(JSON)
	return NewObj
}

func NewJsonArray() JSONarray {
	var NewObj JSONarray
	NewObj = make(JSONarray, 0)
	return NewObj
}

func (j *JSON) Expose(Key string) *JSON {
	var NewObj JSON
	NewObj.IngestFromObject((*j)[Key])
	return &NewObj
}

func (j *JSON) ExposeArray(Key string) JSONarray {
	var NewObj JSONarray
	NewObj.IngestFromObject((*j)[Key])
	return NewObj
}

func (j *JSONarray) String() string {
	s, _ := json.Marshal(*j)
	return string(s)
}

func (j *JSON) Bytes() []byte {
	s, _ := json.Marshal(*j)
	return s
}

func (j *JSONarray) Bytes() []byte {
	s, _ := json.Marshal(*j)
	return s
}

func (j *JSON) String() string {
	if j == nil {
		return "null"
	}
	s, _ := json.Marshal(*j)
	return string(s)
}

func (j *JSON) TemplateString(Str string) string {
	//log.Printf("ConfirmEmailTemplate input %s\n", Str)
	for k, v := range *j {
		Target := fmt.Sprintf("${%s}", k)
		//log.Printf("Substitution for '%s': '%s'\n", Target, v.(string))
		Str = strings.ReplaceAll(Str, Target, v.(string))
	}
	return Str
}
