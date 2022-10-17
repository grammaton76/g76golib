package shared

import (
	"fmt"
	"github.com/go-ini/ini"
	_ "github.com/go-sql-driver/mysql"
	"github.com/grammaton76/g76golib/sjson"
	_ "github.com/lib/pq"
	"os"
	"strings"
)

type Configuration struct {
	IniPath        string
	IniFile        *ini.File
	Override       *Configuration
	Fallback       *Configuration
	vaultPrefix    string
	vaultPrefixSet bool
	//vaultClient    *api.Client
	VaultAddr   string
	secretMap   sjson.JSON
	ChatHandles map[string]*ChatHandle
	DbHandles   map[string]*DbHandle
	AccessHit   SafeIntCache
	AccessMiss  SafeIntCache
	dumpedmap   sjson.JSON
	keyPrefix   string
	failed      error
	warnings    []error
}

func (config *Configuration) SetFallback(File *Configuration) {
	if File == config {
		log.Printf("Just saved you from certain doom, when you attempted to set fallback of an INI file to itself.")
		return
	}
	log.Secretf("Setting fallback on '%s' to check '%s' after\n", config.IniPath, File.IniPath)
	config.Fallback = File
}

func (config *Configuration) SetOverride(File *Configuration) {
	if File == config {
		log.Printf("Just saved you from certain doom, when you attempted to set override of an INI file to itself.")
		return
	}
	log.Secretf("Setting override on '%s' to point to '%s' first\n", config.IniPath, File.IniPath)
	config.Override = File
}

func (config *Configuration) ListSections() []string {
	return config.IniFile.SectionStrings()
}

func (config *Configuration) ListKeys(Path string) []string {
	var Section *ini.Section = config.GetSection(Path)
	var Caw []string
	for _, v := range Section.Keys() {
		Caw = append(Caw, v.Name())
	}
	log.Debugf("Keys detected on path '%s': %v\n", Path, Caw)
	return Caw
}

func (config *Configuration) SectionFromKey(Key string) *sjson.JSON {
	found, caw := config.GetString(Key)
	if !found {
		log.Infof("Failed to fetch section name from key '%s'\n", Key)
		return nil
	}
	Caw := sjson.NewJsonFromObject(config.dumpedmap[caw])
	return Caw
}

func (config *Configuration) Profile() string {
	var buf string
	Bob := config.AccessHit.Export()
	for k, v := range Bob {
		buf += fmt.Sprintf("%s hit %d times\n", k, v)
	}
	Bob = config.AccessMiss.Export()
	for k, v := range Bob {
		buf += fmt.Sprintf("%s MISSED %d times\n", k, v)
	}
	return buf
}

func (config *Configuration) KeyPrefix(Prefix string) {
	config.keyPrefix = Prefix
}

func (config *Configuration) OrDie(format string, msgs ...interface{}) *Configuration {
	if config.failed != nil {
		CustomErr := fmt.Sprintf(format, msgs...)
		log.Fatalf("Fatal error loading config '%s': %s\n%s", config.IniPath, config.failed, CustomErr)
	}
	return config
}

func (config *Configuration) PrintWarnings() *Configuration {
	for _, v := range config.warnings {
		log.Warnf("%s\n", v)
	}
	return config
}

func (config *Configuration) LoadAnIni(Path string) *Configuration {
	Path = EnvParsePath(Path)
	config.IniPath = Path
	cfg, err := ini.Load(Path)
	if err != nil {
		config.failed = fmt.Errorf("failed to read INI file '%s': %s\n", Path, err)
		return config
	}
	config.IniFile = cfg
	config.AccessHit = NewSafeIntCache()
	config.AccessMiss = NewSafeIntCache()
	//log.SetThreshold(DEBUG)
	_, config.VaultAddr = config.GetString("secrets.VAULT_ADDR")
	_, Prefix := config.GetString("secrets.vaultprefix")
	_, Vaults := config.GetString("secrets.vaults")
	found, Fallback := config.GetString("secrets.fallback")
	if found {
		var Secondary Configuration
		Secondary.LoadAnIni(Fallback)
		if Secondary.failed != nil {
			config.warnings = append(config.warnings, fmt.Errorf("failed to add fallback file for '%s': %s", Path, Secondary.failed))
		} else {
			config.SetFallback(&Secondary)
		}
	}
	for _, v := range strings.Split(Vaults, ",") {
		if v == "" {
			continue
		}
		if Prefix != "" {
			config.SetVaultPrefix("")
		}
		err := config.LoadKvOverlay(v)
		if err != nil {
			log.Critf("KV error fetching '%s': %s\n", v, err)
		}

	}
	for _, v := range config.ListKeys("secrets") {
		log.Secretf("We need to iterate over '%s'\n", v)
		if strings.HasPrefix(v, "inikey-") {
			Section := strings.TrimLeft(v, "inikey-")
			VaultKey := config.GetKey("secrets."+v, "")
			if VaultKey == nil {
				log.Errorf("nil section '%s' specified in '%s'; invalid Secrets section!\n",
					Section, v)
			}
			VaultAddr := VaultKey.Value()
			log.Secretf("Need to load Vault stanza '%s' into ini key '%s'\n",
				VaultAddr, Section)
			log.ErrorIff(config.LoadKvOverlayPrefix(VaultAddr, Section), "Failed to load vault overlay '%s' into path '%s'\n",
				VaultAddr, Section)
		}
	}
	config.dumpedmap = config.ExportAsJson()
	return config
}

func (config *Configuration) SetDefaultIni(Path string) *Configuration {
	if os.Getenv("INIFILE") != "" {
		log.Printf("Overriding default INI path '%s' per INIFILE ...\n", config.IniPath)
		Path = os.Getenv("INIFILE")
	}
	return config.LoadAnIni(Path)
}

func (config *Configuration) GetSection(Path string) *ini.Section {
	var Section *ini.Section
	if config.IniFile == nil {
		return nil
	}
	//log.Printf("Looking for ini file path '%s'\n", Path)
	Items := strings.Split(config.keyPrefix+Path, ".")
	for _, v := range Items {
		//log.Printf("Looking for path component '%s'\n", v)
		if Section == nil {
			//log.Printf("Found section '%s'; descending in.\n", v)
			Section = config.IniFile.Section(v)
		} else {
			for _, s := range Section.ChildSections() {
				if s.Name() == v {
					//log.Printf("Found next section '%s'; descending in.\n", v)
					Section = s
				} else {
					//log.Printf("Child section '%s' was not '%s'; continuing to look.\n", s.Name(), v)
				}
			}
		}
	}
	return Section
}

func (config *Configuration) getSecretString(Path string, DieMsg string) (bool, string) {
	if config.secretMap == nil {
		return false, ""
	}
	//log.Printf("Searching for secret string '%s'\n", Path)
	LastDot := strings.LastIndex(Path, ".")
	if LastDot == -1 {
		value, found := config.secretMap[Path]
		//log.Printf("Performing fallback search of secret map.\n")
		if found {
			switch value.(type) {
			case string:
				return true, value.(string)
			default:
				log.Printf("getSecretString: unknown type error on '%s': %s\n", Path, value)
				return false, ""
			}
		}
		return false, ""
	}
	SectionName := Path[:LastDot]
	Section := config.secretMap[SectionName]
	if Section == nil {
		if DieMsg != "" {
			log.Printf("Can't find section '%s' in secret map!\n%s\n", SectionName, DieMsg)
			os.Exit(1)
		} else {
			//log.Printf("Can't find section '%s' in secret map!\n%s\n", SectionName, DieMsg)
			return false, ""
		}
	}
	KeyName := Path[LastDot+1:]
	//	Bob := Section.(map[string]interface{})
	Bob := Section.(sjson.JSON)
	value, found := Bob[KeyName]
	if !found {
		if DieMsg != "" {
			log.Printf("Can't find key '%s.%s' in secret map!\n%s\n", SectionName, KeyName, DieMsg)
			os.Exit(1)
		} else {
			return false, ""
		}
	}
	//log.Printf("We retrieved secret '%s', with value '%s'\n", Path, value)
	return true, value.(string)
}

func (config *Configuration) privGetKey(Path string, DieMsg string) *ini.Key {
	if config == nil {
		log.Secretf("Referred privGetKey down to nil config for path '%s'.\n", Path)
		return nil
	}
	var Section *ini.Section
	log.Secretf("privGetKey looking in '%s' for path '%s'\n", config.IniPath, Path)
	LastDot := strings.LastIndex(Path, ".")
	if LastDot == -1 {
		Bob := config.IniFile.Section("")
		if Bob == nil {
			return config.Fallback.privGetKey(Path, DieMsg)
		}
		if Bob.HasKey(Path) {
			log.Secretf("Found key '%s' in '%s'\n", Path, config.IniPath)
			return Bob.Key(Path)
		}
		return config.Fallback.privGetKey(Path, DieMsg)
	}
	SectionName := Path[:LastDot]
	Section = config.GetSection(SectionName)
	if Section == nil {
		if DieMsg != "" {
			log.Printf("Can't find section '%s' in ini!\n%s\n", SectionName, DieMsg)
			os.Exit(1)
		} else {
			return config.Fallback.privGetKey(Path, DieMsg)
		}
	}
	KeyName := Path[LastDot+1:]
	if !Section.HasKey(KeyName) {
		if DieMsg != "" {
			log.Printf("Can't find key '%s.%s' in ini!\n%s\n", SectionName, KeyName, DieMsg)
			os.Exit(1)
		} else {
			return config.Fallback.privGetKey(Path, DieMsg)
		}
	}
	log.Secretf("GetKey found key '%s': '%s'\n", KeyName, Section.Key(KeyName))
	return Section.Key(KeyName)
}

func (config *Configuration) GetKey(Path string, DieMsg string) (Caw *ini.Key) {
	Path = config.keyPrefix + Path
	if config.Override != nil {
		log.Secretf("Checking override first for %s\n", Path)
		Caw = config.Override.privGetKey(Path, "")
		if Caw != nil {
			config.AccessHit.Inc(Path)
			return Caw
		}
	}
	Caw = config.privGetKey(Path, DieMsg)
	if Caw == nil {
		if config.Fallback != nil {
			Caw = config.Fallback.GetKey(Path, DieMsg)
		}
	}
	if Caw == nil {
		config.AccessMiss.Inc(Path)
	} else {
		config.AccessHit.Inc(Path)
	}
	return Caw
}

func (config *Configuration) GetString(Path string) (bool, string) {
	Key := config.GetKey(Path, "")
	if Key == nil {
		found, value := config.getSecretString(Path, "")
		if found {
			return true, value
		}
		return false, ""
	} else {
		log.Secretf("Key for %s: '%s'\n", Path, Key)
	}
	return true, Key.String()
}

func (config *Configuration) GetStringOrDefault(Path string, Default string, DefaultMessage string, Options ...interface{}) string {
	Key := config.GetKey(Path, "")
	if Key == nil {
		found, value := config.getSecretString(Path, "")
		if found {
			return value
		}
		if DefaultMessage == "" {
			log.Infof("No config value defined for '%s'; defaulting to '%s'\n", Path, Default)
		} else {
			var Because = fmt.Sprintf(DefaultMessage, Options...)
			log.Infof("No value for '%s'; defaulted to '%s' because %s", Path, Default, Because)
		}
		return Default
	} else {
		log.Secretf("Key for %s: '%s'\n", Path, Key)
		return Key.String()
	}
}

func (config *Configuration) GetFloat(Path string) (bool, float64) {
	Key := config.GetKey(Path, "")
	if Key == nil {
		return false, 0
	}
	Num, Err := Key.Float64()
	if Err == nil {
		return true, Num
	} else {
		return false, Num
	}
}

func (config *Configuration) GetFloatOrDie(Path string, DieMsg string) float64 {
	found, Value := config.GetFloat(Path)
	if !found {
		log.Fatalf("Failed to fetch '%s' from '%s': %s\n", Path, config.IniPath, DieMsg)
	}
	return Value
}

func (config *Configuration) GetBool(Path string) (bool, bool) {
	found, Val := config.GetString(Path)
	if !found {
		return false, false
	}
	switch strings.ToLower(Val) {
	case "1", "true":
		return true, true
	case "0", "false":
		return true, false
	}
	return false, false
}

func (config *Configuration) GetBoolOrDefault(Path string, Default bool, DefaultMessage string, Options ...interface{}) bool {
	value, found := config.GetBool(Path)
	if found {
		return value
	}
	if DefaultMessage == "" {
		log.Infof("No config value defined for '%s'; defaulting to '%t'\n", Path, Default)
	} else {
		var Because = fmt.Sprintf(DefaultMessage, Options...)
		log.Infof("No value for '%s'; defaulted to '%t' because %s", Path, Default, Because)
	}
	return Default
}

func (config *Configuration) GetBoolOrDie(Path string, DieMsg string) bool {
	found, Value := config.GetBool(Path)
	if !found {
		log.Fatalf("Failed to fetch '%s' from '%s': %s\n", Path, config.IniPath, DieMsg)
	}
	return Value
}

func (config *Configuration) GetInt(Path string) (bool, int) {
	Key := config.GetKey(Path, "")
	if Key == nil {
		return false, 0
	}
	Num, Err := Key.Int()
	if Err == nil {
		return true, Num
	} else {
		return false, Num
	}
}

func (config *Configuration) GetInt64(Path string) (bool, int64) {
	Key := config.GetKey(Path, "")
	if Key == nil {
		return false, 0
	}
	Num, Err := Key.Int64()
	if Err == nil {
		return true, Num
	} else {
		return false, Num
	}
}

func (config *Configuration) GetIntOrDie(Path string, DieMsg string) int {
	found, Value := config.GetInt(Path)
	if !found {
		log.Fatalf("Failed to fetch '%s' from '%s': %s\n", Path, config.IniPath, DieMsg)
	}
	return Value
}

func (config *Configuration) GetInt64OrDefault(Path string, Default int64, DefaultMessage string, Options ...interface{}) int64 {
	found, value := config.GetInt64(Path)
	if found {
		return value
	}
	if DefaultMessage == "" {
		log.Infof("No config value defined for '%s'; defaulting to '%s'\n", Path, Default)
	} else {
		var Because = fmt.Sprintf(DefaultMessage, Options...)
		log.Infof("No value for '%s'; defaulted to '%d' because %s\n", Path, Default, Because)
	}
	return Default
}

func (config *Configuration) GetIntOrDefault(Path string, Default int, DefaultMessage string, Options ...interface{}) int {
	log.DepthOffsetRel(+1)
	defer log.DepthOffsetRel(-1)
	found, value := config.GetInt(Path)
	if found {
		return value
	}
	if DefaultMessage == "" {
		log.Infof("No config value defined for '%s'; defaulting to '%s'\n", Path, Default)
	} else {
		var Because = fmt.Sprintf(DefaultMessage, Options...)
		log.Infof("No value for '%s'; defaulted to '%d' because %s\n", Path, Default, Because)
	}
	return Default
}

func (config *Configuration) GetStringOrDie(Path string, DieMsg string, options ...interface{}) string {
	log.DepthOffsetRel(+1)
	defer log.DepthOffsetRel(-1)
	found, Value := config.GetString(Path)
	if !found {
		DieMsg = fmt.Sprintf(DieMsg, options...)
		log.Fatalf("Failed to fetch '%s' from '%s': %s\n", Path, config.IniPath, DieMsg)
	}
	return Value
}

func (config *Configuration) ConnectDbKey(Key string) *DbHandle {
	found, Section := config.GetString(Key)
	if !found {
		Caw := DbHandle{
			Key:    Key,
			failed: fmt.Errorf("key %s not found", Key),
		}
		return &Caw
	}
	Caw := config.configDbhFromSection(Section)
	Caw.Key = Key
	log.ErrorIff(Caw.Connect(), "error connecting on key %s\n", Caw.Identifier())
	return Caw
}

func (config *Configuration) DefineDbFromKey(Key string) *DbHandle {
	found, Section := config.GetString(Key)
	if !found {
		Caw := DbHandle{
			Key:    Key,
			failed: fmt.Errorf("key %s not found", Key),
		}
		return &Caw
	}
	Caw := config.configDbhFromSection(Section)
	return Caw
}

func (config *Configuration) DefineDbFromSection(Section string) *DbHandle {
	return config.configDbhFromSection(Section)
}

func addDbKeyWarning(db *DbHandle, Key string, Fmt string, Opt ...interface{}) {
	db.warnings = append(db.warnings, fmt.Errorf("config key '%s': %s", Key, fmt.Sprintf(Fmt, Opt...)))
}

func (config *Configuration) Identifier() string {
	return fmt.Sprintf("ini file '%s'", config.IniPath)
}

func (config *Configuration) configDbhFromSection(Section string) *DbHandle {
	var DefaultDb string = "mysql"
	//log.Printf("Passed '%s' as section in populate\n", Section)
	if Section == "" {
		log.Printf("Attempted to populate a db connection with a blank section. Fail.\n")
		return nil
	}
	Found, DbType := config.GetString(Section + ".dbtype")
	if !Found {
		log.Printf("Key '%s.dbtype' in '%s' has unknown value '%s'; defaulting to type '%s'!\n",
			Section, config.IniPath, DbType, DefaultDb)
		DbType = DefaultDb
	}
	log.Debugf("Now passing through to connect database type '%s' via section '%s' of '%s'\n", DbType, Section, config.IniPath)

	var Caw DbHandle
	Caw.Section = Section
	_, err := config.ListedKeysPresent(Section+".dbhost", Section+".dbname", Section+".dbuser", Section+".dbpass")
	if err != nil {
		Caw.failed = err
		return &Caw
	}
	var found bool
	found, Caw.Host = config.GetString(Section + ".dbhost")
	if !found {
		addDbKeyWarning(&Caw, Section+"dbhost", "missing")
	}
	found, Caw.DbName = config.GetString(Section + ".dbname")
	if !found {
		addDbKeyWarning(&Caw, Section+"dbname", "missing")
	}
	found, Caw.Username = config.GetString(Section + ".dbuser")
	if !found {
		addDbKeyWarning(&Caw, Section+"dbuser", "missing")
	}
	found, Caw.Password = config.GetString(Section + ".dbpass")
	if !found {
		addDbKeyWarning(&Caw, Section+"dbpass", "missing")
	}
	switch DbType {
	case "pgsql":
		Caw.dbtype = DbTypePostgres
	case "mysql":
		Caw.dbtype = DbTypeMysql
	default:
		Caw.failed = fmt.Errorf("Failed to load db handle from section '%s'; warnings: %s\n",
			Section, Caw.warnings)
		return &Caw
	}
	return &Caw
}

func (config *Configuration) ConnectDbBySection(SectionName string) *DbHandle {
	if config.DbHandles == nil {
		config.DbHandles = make(map[string]*DbHandle)
	}
	log.Debugf("Now searching ini file '%s' for database handle '%s'\n", config.IniPath, SectionName)
	if config.DbHandles[SectionName] != nil {
		if config.DbHandles[SectionName].Ping() == nil {
			return config.DbHandles[SectionName]
		}
	}
	dbh := config.configDbhFromSection(SectionName)
	config.DbHandles[SectionName] = dbh
	err := dbh.Connect()
	if err != nil {
		dbh.failed = fmt.Errorf("Failed to connect to db section '%s': %s\n", dbh.Identifier(), err)
	}
	return dbh
}

func (config *Configuration) ConnectDbBySectionOrDie(SectionName string) *DbHandle {
	var Return *DbHandle
	Return = config.ConnectDbBySection(SectionName)
	if Return == nil {
		log.Fatalf("Failed to connect to database handle '%s' in '%s'; exiting.\n", SectionName, config.IniPath)
		os.Exit(1)
	}
	err := Return.Ping()
	log.FatalIff(err, "DB ping failed for handle '%s'\n", Return.Identifier())
	return Return
}

func (config *Configuration) ListedKeysPresent(Keys ...string) (bool, error) {
	for _, v := range Keys {
		//log.Printf("Looking for '%s'\n", v)
		if config.GetKey(v, "") != nil {
			return true, nil
		}
		SecretFound, _ := config.getSecretString(v, "")
		if !SecretFound {
			return false, fmt.Errorf("key '%s' was missing from config AND secrets", v)
		}
	}
	return true, nil
}

func (config *Configuration) NewMailHandle(Section string) *MailHandle {
	var Caw MailHandle
	var found bool
	found, Caw.ActuallySend = config.GetBool(Section + ".actuallysend")
	if found {
		log.Debugf("Mail handle %s has %t defined for actually-send mode.\n", Section, Caw.ActuallySend)
	} else {
		log.Errorf("Nothing defined for '%s.actuallysend'; returning a nil mail handle\n", Section)
		return nil
	}
	_, Caw.DefaultRecipient = config.GetString(Section + ".defaultrecipient")
	_, Caw.SubjectPrefix = config.GetString(Section + ".subjectprefix")
	_, Caw.DefaultSender = config.GetString(Section + ".defaultsender")
	log.Debugf("Mail handle instantiated with settings: %#v\n", Caw)
	return &Caw
}

func addChatKeyWarning(cth *ChatHandle, Key string, Fmt string, Opt ...interface{}) {
	err := fmt.Errorf("config key '%s': %s", Key, fmt.Sprintf(Fmt, Opt...))
	log.Debugf("%s setup warning: %s\n", cth.Identifier(), err)
	cth.warnings = append(cth.warnings, err)
}

func (config *Configuration) NewChatHandle(Section string) *ChatHandle {
	return config.newChatHandle(Section)
}

func (config *Configuration) ChatTargetFromKey(Key string) *ChatTarget {
	found, Identifier := config.GetString(Key)
	if !found {
		log.Debugf("No value at '%s'; no chat target is being assigned.\n", Key)
		return nil
	}
	Parsed := strings.Split(Identifier, ":")
	var Channel string
	sHandle := Parsed[0]
	if len(Parsed) > 1 {
		Channel = Parsed[1]
	}
	Handle := config.NewChatHandle(sHandle)
	if Handle == nil {
		log.Errorf("ChatTargetFromKey: Failed to spin up chat handle from section '%s'\n",
			sHandle)
		return nil
	}
	if len(Parsed) == 1 {
		return Handle.OutputChannel
	}
	tgt := Handle.ChatTargetChannel(Channel)
	return tgt
}

func (config *Configuration) newChatHandle(Section string) *ChatHandle {
	var Caw *ChatHandle
	Defer := sjson.NewJson()
	if config.ChatHandles[Section] != nil {
		log.Debugf("Found a cached instance of chat connection for '%s'\n", Section)
		return config.ChatHandles[Section]
	}
	log.Debugf("Setting up chat based upon INI section '%s'\n", Section)
	if config.ChatHandles == nil {
		config.ChatHandles = make(map[string]*ChatHandle)
	}
	Caw = &ChatHandle{}
	Caw.section = Section
	Caw.UserIndex.ById = make(map[string]*UserInfo)
	Caw.ChannelLookup = make(map[string]*ChatTarget)
	Caw.PrintChatOnly = config.GetBoolOrDefault(Section+".printonly", false, "")
	found, val := config.GetString(Section + ".chattype")
	if found {
		Caw.ChatType = ChatTypeFromString(val)
	} else {
		Caw.ChatType = ChatTypeUndef
		log.Fatalf("Config item '%s.chattype' not defined\n", Section)
	}
	CTC := ChatHandleConfigs[Caw.ChatType.String()]
	if CTC != nil {
		err := CTC.BindFunc(Caw, config, Section)
		log.ErrorIff(err, "Binding function failed for '%s'", Caw.Identifier())
	} else {
		log.Printf("ChatHandle: ChatType was nil; ensure the driver for type '%s' is loaded..\n",
			val)
	}
	Caw.deferConfig = &Defer
	if !Caw.PrintChatOnly {
		if Caw.DirectClient == nil {
			//log.Fatalf("No output method in section '%s'; neither dbhandle nor directhandle is valid\n", Section)
		}
	}
	found, AsUser := config.GetString(Section + ".chathandle")
	if found {
		Caw.SetDefaultSender(AsUser)
	} else {
		log.Debugf("Chat handle setup: '%s.chathandle' not found.\n", Section)
	}
	found, OutputChannel := config.GetString(Section + ".channel")
	if found {
		Caw.SetDefaultChannel(OutputChannel)
	} else {
		log.Debugf("Chat handle setup: '%s.channel' not found.\n", Section)
	}
	found, ErrorChannel := config.GetString(Section + ".errorchannel")
	if found {
		Caw.SetErrorChannel(ErrorChannel)
	} else {
		log.Debugf("Chat handle setup: '%s.errorchannel' not found.\n", Section)
	}
	return Caw
}

func (config *Configuration) GetSecretOrDie(query string) sjson.JSON {
	Success, Value := config.GetSecret(query)
	if Success == false {
		log.Fatalf("See previous message - retrieval failed in GetSecretOrDie; exiting.\n")
	}
	return Value
}

func (config *Configuration) SetVaultPrefix(Prefix string) {
	config.vaultPrefix = Prefix
	config.vaultPrefixSet = true
	log.ErrorIff(config.connectVault(), "Vault connection")
}

func (config *Configuration) GetSecret(query string) (bool, sjson.JSON) {
	log.Printf("We're looking for a secret at '%s'\n", query)
	return false, nil
}

func (config *Configuration) writeSpiderSecretsFromMap(Obj map[string]interface{}, Target *sjson.JSON) {
	//log.Printf("writeSpiderSecretsFromMap starting up.\n")
	for k, v := range Obj {
		switch v.(type) {
		case map[string]interface{}:
			//log.Printf("... key '%s' is a map. writeSpiderSecrets descending into further madness.\n", k)
			Bob := make(sjson.JSON)
			config.writeSpiderSecrets(v.(sjson.JSON), &Bob)
			(*Target)[k] = Bob
			//log.Printf("Spidered result is %s\n", Bob)
		default:
			//log.Printf("writeSpiderSecretsFromMap: Who knows what %s should do?\n", k)
			(*Target)[k] = v
		}
	}
	//log.Printf("Finished writeSpiderSecrets section; results are %v\n", *Target)
}

func (config *Configuration) writeSpiderSecrets(Obj sjson.JSON, Target *sjson.JSON) {
	for k, v := range Obj {
		switch v.(type) {
		case map[string]interface{}:
			//log.Printf("... key '%s' is a map. writeSpiderSecrets descending into further madness.\n", k)
			var Bob sjson.JSON
			if (*Target)[k] != nil {
				Bob = (*Target)[k].(sjson.JSON)
			} else {
				Bob.New()
			}
			config.writeSpiderSecretsFromMap(v.(map[string]interface{}), &Bob)
			(*Target)[k] = Bob
			//log.Printf("Spidered result is %s\n", Bob)
		default:
			//	log.Printf("writeSpiderSecrets: Who knows what %s should do?\n", k)
			(*Target)[k] = v
		}
	}
	//log.Printf("Finished writeSpiderSecrets section; results are %v\n", *Target)
}

func (config *Configuration) ExportSectionAsJson(Section string) sjson.JSON {
	var Output sjson.JSON
	Output.New()
	//log.Printf("Secret map: %+v\n", config.secretMap)
	if config.Fallback != nil {
		Output.SpiderCopyJsonFrom(config.Fallback.secretMap)
	}
	Output.SpiderCopyJsonFrom(config.secretMap)
	//log.Printf("SECRET OUTPUT after spidercopyjson: %+v\n", Output.ExportAsIniString())
	if config.Fallback != nil {
		Output.SpiderCopyIniFrom(config.Fallback.IniFile)
	}
	Output.SpiderCopyIniFrom(config.IniFile)
	//log.Printf("FINAL OUTPUT output map: %+v\n", Output.ExportAsIniString())
	// Need to output the INI too
	return Output
}

func (config *Configuration) ExportAsJson() sjson.JSON {
	var Output sjson.JSON
	Output.New()
	//log.Printf("Secret map: %+v\n", config.secretMap)
	if config.Fallback != nil {
		Output.SpiderCopyJsonFrom(config.Fallback.secretMap)
	}
	Output.SpiderCopyJsonFrom(config.secretMap)
	//log.Printf("SECRET OUTPUT after spidercopyjson: %+v\n", Output.ExportAsIniString())
	if config.Fallback != nil {
		Output.SpiderCopyIniFrom(config.Fallback.IniFile)
	}
	Output.SpiderCopyIniFrom(config.IniFile)
	//log.Printf("FINAL OUTPUT output map: %+v\n", Output.ExportAsIniString())
	// Need to output the INI too
	return Output
}

func (config *Configuration) LoadKvOverlay(VaultPath string) error {
	return config.LoadKvOverlayPrefix(VaultPath, "")
}

func (config *Configuration) LoadKvOverlayPrefix(VaultPath string, DestPrefix string) error {
	/*
		vaultLogical := config.GetVaultLogical()
		Fullsearch := config.vaultPrefix + VaultPath
		log.Debugf("Loading KV store '%s' into config object\n", Fullsearch)
		secret, err := vaultLogical.Read(Fullsearch)
		if err != nil {
			log.Printf("KV vault read error: %s\n", err)
			return err
		}
		if secret == nil {
			return fmt.Errorf("no secrets to export at '%s'\n", VaultPath)
		}
		if config.secretMap == nil {
			config.secretMap.New()
		}
		if DestPrefix != "" {
			Caw := sjson.NewJson()
			config.writeSpiderSecrets(secret.Data, &Caw)
			config.secretMap[DestPrefix] = Caw
		} else {
			config.writeSpiderSecrets(secret.Data, &config.secretMap)
		}
		//log.Printf("Secrets ingested to map; full secret map is now: %+v\n", config.secretMap)
	*/
	return nil
}

//func (config *Configuration) GetVaultLogical() *api.Logical {
//if config.vaultClient != nil {
//	return config.vaultClient.Logical()
//}
//config.connectVault()
//return config.vaultClient.Logical()
//}

func EnvParsePath(Path string) (Ret string) {
	Ret = strings.ReplaceAll(Path, "$HOME", os.Getenv("HOME"))
	return Ret
}

func (config *Configuration) connectVault() error {
	/*
		var token = os.Getenv("VAULT_TOKEN")
		if token == "" {
			Home := os.Getenv("HOME")
			if Home != "" {
				TokenFile := fmt.Sprintf("%s/.vault-token", Home)
				dat, err := ioutil.ReadFile(TokenFile)
				if err == nil {
					token = string(dat)
				}
			}
		}
		if config.VaultAddr == "" {
			config.VaultAddr = os.Getenv("VAULT_ADDR")
			if config.VaultAddr == "" {
				config.VaultAddr = "http://localhost:8200/"
			}
		}
		//	log.Printf("Vault is '%s'; token is '%s'\n", vault_addr, token)
		var err error
		VaultConfig := &api.Config{
			Address: config.VaultAddr,
		}
		Client, err := api.NewClient(VaultConfig)
		if err != nil {
			fmt.Println(err)
			return err
		}
		Client.SetToken(token)
		//config.vaultClient = Client */
	return nil
}
