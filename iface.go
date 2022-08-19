package g76golib

import (
	"encoding/json"
	"github.com/grammaton76/g76golib/slogger"
	"strconv"
)

var log *slogger.Logger

type Iface struct {
	Top interface{}
}

type Key struct {
	Parent *Iface
	Path   string
}

func (Source *Iface) Key(Path string) Key {
	return Key{Parent: Source, Path: Path}
}

func (Target Key) Raw() interface{} {
	Caw := (Target.Parent.Top).(map[string]interface{})
	return Caw[Target.Path]
}

func (Target Key) String() string {
	Caw := Target.Raw()
	return Caw.(string)
}

func (Target Key) Float() float64 {
	Val := Target.Raw()
	switch Type := Val.(type) {
	case string:
		if s, err := strconv.ParseFloat(Val.(string), 64); err == nil {
			return s
		} else {
			log.Warnf("path '%s' float failed to parse: '%s'\n", Target.Path, err)
		}
	default:
		log.Warnf("path '%s' maps to type '%s', no defined path to a float for value: %v!\n",
			Target.Path, Type, Val)
	}
	return 0
}

func (Source *Iface) Absorb(Input interface{}) {
	//log.Printf("Attempting to absorb the target...\n")
	Source.Top = Input
	/*Type:=reflect.TypeOf(Input)
	switch Type {
	default:
		log.Fatalf("Don't know how to Absorb() type '%s'\n", Type)
	}*/
}

func (Source *Iface) SplitArray() []Iface {
	var Result []Iface
	for _, v := range Source.Top.([]interface{}) {
		var Caw Iface
		Caw.Top = v
		Result = append(Result, Caw)
	}
	return Result
}

func (Source *Iface) Bytes() []byte {
	res, err := json.Marshal(Source.Top)
	if err != nil {
		log.Printf("Error encoding iface to bytes: '%s'!\n", err)
	}
	return res
}

func (Source *Iface) String() string {
	return string(Source.Bytes())
}
