package slogger // Package slogger import "github.com/grammaton76/g76golib/slogger"

import (
	"fmt"
	"github.com/kardianos/osext"
	"io"
	real_log "log"
	"net"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

const (
	TIMEFORMAT_SECONDS_WITH_TZ = "2006-01-02 15:04:05 MST"
)

var log *Logger

var ExitProfiling bool

type LogLevel int

type Logger struct {
	MinLevel          LogLevel
	MinBufferLevel    LogLevel
	TraceAbove        LogLevel
	LineAbove         LogLevel
	LogBase           string
	logRendered       string
	logFile           *os.File
	RealLogger        *real_log.Logger
	BufferLineLimit   int
	BufferSizeLimit   int
	Buffer            []LogEntry
	bufferSizeInBytes int
	LinenumFormat     string
	depthoffset       int
}

type LogEntry struct {
	Time  time.Time
	Text  string
	Level LogLevel
}

const (
	LstdFlags     int = real_log.LstdFlags
	Lmicroseconds int = real_log.Lmicroseconds
)

var SysBaseLogPath string = "/data/logs"

const (
	SPAM   LogLevel = -30
	SECRET LogLevel = -20
	DEBUG  LogLevel = -10
	INFO   LogLevel = 0
	WARN   LogLevel = 10
	ERROR  LogLevel = 20
	CRIT   LogLevel = 30
	ELE    LogLevel = 100
)

var logLevelToString = map[LogLevel]string{
	SPAM:   "SPAM",
	SECRET: "SECRET",
	DEBUG:  "DEBUG",
	INFO:   "INFO",
	WARN:   "WARN",
	ERROR:  "ERROR",
	CRIT:   "CRIT",
}

var logLevelMap = map[string]LogLevel{
	"SPAM":   SPAM,
	"SECRET": SECRET,
	"DEBUG":  DEBUG,
	"INFO":   INFO,
	"WARN":   WARN,
	"ERROR":  ERROR,
	"CRIT":   CRIT,
}

func SetLogger(l *Logger) *Logger {
	log = l
	return l
}

func init() {
	log = &Logger{}
}

func DeferCloseBodyErr(Body io.ReadCloser, label string) {
	log.ErrorIff(Body.Close(), "Closing response body '%s'", label)
}

func newRealLogger() *real_log.Logger {
	return real_log.New(os.Stderr, "", real_log.LstdFlags|real_log.Lshortfile)
}

func NewLogger() *Logger {
	Me := Logger{MinLevel: INFO, RealLogger: newRealLogger()}
	Me.SetBaseName(MyExecBaseName())
	return &Me
}

func SetProfile(label string, bob interface{}) {
	if profileObjects == nil {
		profileObjects = make(map[string]*interface{})
	}
	profileObjects[label] = &bob
}

var profileObjects map[string]*interface{}

func Exit(code int) {
	if log.MinLevel <= DEBUG {
		ExitProfiling = true
	}
	if ExitProfiling {
		for k, v := range profileObjects {
			Type := reflect.TypeOf(*v).String()
			log.Printf("Now profiling '%s' (%s)\n", k, Type)
			switch Type {
			default:
				log.Debugf("No idea how to profile object of type '%s'\n", Type)
			}
		}
	}
	os.Exit(code)
}

func MyExecBaseName() string {
	name, err := osext.Executable()
	if err != nil {
		log.Printf("Failed to get executable path for checksum: %s!\n", err)
		os.Exit(1)
	}
	KillPath := regexp.MustCompile(".*/")
	name = KillPath.ReplaceAllString(name, "")
	StripDevName := regexp.MustCompile("___go_build_(.*)_go")
	Caw := StripDevName.FindStringSubmatch(name)
	if len(Caw) == 2 {
		return Caw[1]
	}
	return name
}

func (l *Logger) ensureCurrentLogFile() {
	NewName := l.LogName()
	if NewName != l.logRendered {
		l.ensureRealLogger()
		if l.logFile != nil {
			l.RealLogger.Printf("Rotating log file to '%s'\n", NewName)
			l.logFile.Close()
		}
		f, err := os.OpenFile(NewName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			l.RealLogger.Printf("Error opening log file '%s': %v", NewName, err)
		}
		l.logFile = f
		mw := io.MultiWriter(os.Stdout, f)
		l.RealLogger.SetOutput(mw)
		l.logRendered = NewName
	}
}

func (l *Logger) SetBaseName(Base string) {
	if Base == "" {
		l.LogBase = fmt.Sprintf("%s/%s", SysBaseLogPath, MyExecBaseName())
	} else if strings.HasPrefix(Base, "/") {
		l.LogBase = Base
	} else {
		l.LogBase = fmt.Sprintf("%s/%s", SysBaseLogPath, Base)
	}
	log.Debugf("Set log base path to '%s'\n", l.LogBase)
	l.ensureCurrentLogFile()
	//log.SetOutput(f)
}

func (l *Logger) LogName() string {
	if l.LogBase == "" {
		l.SetBaseName("")
	}
	Name := fmt.Sprintf("%s-%s_%s", l.LogBase, time.Now().Format("2006-01-02"), os.Getenv("USER"))
	return Name
	//log.SetOutput(f)
}

func (l *Logger) RelayServer(c net.Conn) {
	log.Printf("Local log socket connected [%s]", c.RemoteAddr().Network())
	r := strings.NewReader(l.BufferAsHtml())
	io.Copy(c, r)
	c.Close()
}

func (l *Logger) ServeLogSocket(Path string) {
	if err := os.RemoveAll(Path); err != nil {
		l.Fatalf("%s\n", err)
	}
	listener, err := net.Listen("unix", Path)
	if err != nil {
		log.Fatalf("listen error:", err)
	}
	defer listener.Close()

	for {
		// Accept new connections, dispatching them to RelayServer
		// in a goroutine.
		conn, err := listener.Accept()
		if err != nil {
			l.Fatalf("accept error:", err)
		}
		go l.RelayServer(conn)
	}
}

func (l *Logger) SetLineAbove(Level LogLevel) *Logger {
	l.LineAbove = Level
	return l
}

func (l *Logger) SetFlags(Flags int) *Logger {
	l.RealLogger.SetFlags(Flags)
	return l
}

func (l *Logger) SetBufferLines(x int) *Logger {
	l.BufferLineLimit = x
	l.BufferSizeLimit = 0
	return l
}

func (l *Logger) SetBufferSize(x int) *Logger {
	l.BufferLineLimit = 0
	l.BufferSizeLimit = x
	return l
}

func (l *Logger) LogLevelFromString(Level string) LogLevel {
	if val, ok := logLevelMap[Level]; ok {
		l.MinLevel = val
		l.Debugf("Setting log level to %s (%d)\n", Level, val)
		return val
	}
	l.MinLevel = DEBUG
	return DEBUG
}

func (l *Logger) SetThreshold(Level LogLevel) *Logger {
	l.MinLevel = Level
	return l
}

func (l *Logger) SetTraceAbove(Level LogLevel) *Logger {
	l.TraceAbove = Level
	return l
}

func (l *Logger) SetOutput(w io.Writer) *Logger {
	l.RealLogger.SetOutput(w)
	return l
}

func (l *Logger) AddLogEntry(Entry LogEntry) *Logger {
	if l.BufferSizeLimit == 0 && l.BufferLineLimit == 0 {
		return l
	}
	l.Buffer = append(l.Buffer, Entry)
	l.bufferSizeInBytes += len(Entry.Text)
	if l.BufferSizeLimit > 0 {
		for l.bufferSizeInBytes > l.BufferSizeLimit {
			l.bufferSizeInBytes -= len(l.Buffer[0].Text)
			l.Buffer = l.Buffer[1:]
		}
	}
	if l.BufferLineLimit > 0 {
		for len(l.Buffer) > l.BufferLineLimit {
			l.Buffer = l.Buffer[1:]
		}
	}
	return l
}

func (l *Logger) ensureRealLogger() {
	if l.RealLogger == nil {
		l.RealLogger = newRealLogger()
		l.RealLogger.SetFlags(real_log.Lshortfile | real_log.LstdFlags)
		l.Debugf("Had to instantiate a new slogger instance.\n")
		l.ensureRealLogger()
	}
}

func (l *Logger) coreIff(Depth int, err error, Level LogLevel, format string, options ...interface{}) *Logger {
	if err == nil {
		return l
	}
	format = fmt.Sprintf("%s yielded error '%s'\n", format, err)
	if Level < l.MinLevel && Level < l.MinBufferLevel {
		return l
	}
	l.ensureRealLogger()
	if Level > l.TraceAbove {
		l.RealLogger.Printf("===STACK=== (level %d; threshold was %d)\n%s\n===ENDSTACK===\n", Level, l.TraceAbove, string(debug.Stack()))
	}
	if Level >= l.MinLevel {
		LevelStr := LogLevelString(Level)
		if LevelStr == "" {
			LevelStr = "NONE"
		}
		log.printf(2+Depth, LevelStr, format, options...)
	}
	if Level >= l.MinBufferLevel {
		var Entry LogEntry
		Entry.Time = time.Now()
		Format := fmt.Sprintf("%s %s", LogLevelString(Level), format)
		Entry.Text = fmt.Sprintf(Format, options...)
		Entry.Level = Level
		l.AddLogEntry(Entry)
	}
	return l
}

func (l *Logger) Coref(Level LogLevel, format string, options ...interface{}) bool {
	if Level < l.MinLevel && Level < l.MinBufferLevel {
		return false
	}
	if l.RealLogger == nil {
		l.RealLogger = newRealLogger()
		l.RealLogger.SetFlags(real_log.Lshortfile | real_log.LstdFlags)
		l.SetBaseName(MyExecBaseName())
		l.Errorf("Had to instantiate a new slogger instance; we were passed an empty one.\n")
	}
	if Level > l.TraceAbove {
		l.RealLogger.Printf("===STACK=== (level %d; threshold was %d)\n%s\n===ENDSTACK===\n", Level, l.TraceAbove, string(debug.Stack()))
	}
	l.ensureCurrentLogFile()
	if Level >= l.MinLevel {
		LevelStr := LogLevelString(Level)
		if LevelStr == "" {
			LevelStr = "NONE"
		}
		log.printf(4, LevelStr, format, options...)
	}
	if Level >= l.MinBufferLevel {
		var Entry LogEntry
		Entry.Time = time.Now()
		Format := fmt.Sprintf("%s %s", LogLevelString(Level), format)
		Entry.Text = fmt.Sprintf(Format, options...)
		Entry.Level = Level
		l.AddLogEntry(Entry)
	}
	return true
}

func LogLevelString(Level LogLevel) string {
	if logLevelToString[Level] != "" {
		return logLevelToString[Level]
	}
	return "UNDEF"
}

func (l *Logger) BufferAsHtml() string {
	var Buf string = "<table border=\"1\">\n"
	for _, v := range l.Buffer {
		Buf += fmt.Sprintf("<tr><td>%s</td><td>%s</td><td><xmp>%s</xmp></td></tr>",
			TimeWithTz(v.Time), logLevelToString[v.Level], v.Text)
	}
	Buf += "</table>\n"
	return Buf
}

func TimeWithTz(Time time.Time) string {
	return Time.Format(TIMEFORMAT_SECONDS_WITH_TZ)
}

func (l *Logger) Printf(format string, options ...interface{}) {
	l.printf(3, "NONE", format, options...)
}

func (l *Logger) PrintIff(err error, format string, options ...interface{}) {
	if err != nil {
		l.printf(3, "NONE", format, options...)
	}
}

func LineInfo(calldepth int, format string) string {
	var ok bool
	_, file, line, ok := runtime.Caller(calldepth)
	if !ok {
		file = "???"
		line = 0
	}
	return fmt.Sprintf(format, file, line)
}

func (l *Logger) SetLineFormat(format string) *Logger {
	l.LinenumFormat = format
	return l
}

func (l *Logger) DepthOffsetRel(i int) {
	l.depthoffset += i
}

func (l *Logger) DepthOffsetAbs(i int) {
	l.depthoffset = i
}

func (l *Logger) Init() error {
	if l.RealLogger == nil {
		l.RealLogger = newRealLogger()
	}
	return nil
}

func (l *Logger) printf(Depth int, level string, format string, options ...interface{}) {
	if l.RealLogger == nil {
		l.RealLogger = newRealLogger()
	}
	Format := fmt.Sprintf("%-5s %s", level, format)
	l.AddLogEntry(LogEntry{
		Time:  time.Now(),
		Text:  fmt.Sprintf(Format, options...),
		Level: INFO,
	})
	Buf := fmt.Sprintf(Format, options...)
	l.ensureCurrentLogFile()
	l.RealLogger.Output(Depth, Buf)
}

func (l *Logger) FatalTracef(format string, options ...interface{}) {
	l.TraceAbove = SPAM
	l.Coref(CRIT, format, options...)
	os.Exit(1)
}

func (l *Logger) Panicf(format string, options ...interface{}) {
	l.TraceAbove = ELE
	l.Coref(CRIT, format, options...)
	os.Exit(1)
}

func (l *Logger) Fatalf(format string, options ...interface{}) {
	l.Coref(ERROR, format, options...)
	os.Exit(11)
}

func (l *Logger) FatalInfoIff(err error, format string, options ...interface{}) {
	if err != nil {
		l.coreIff(2, err, INFO, format, options...)
		os.Exit(0)
	}
}

func (l *Logger) FatalIff(err error, format string, options ...interface{}) {
	if err != nil {
		l.coreIff(2, err, CRIT, format, options...)
		os.Exit(11)
	}
}

func (l *Logger) ErrorIff(err error, format string, options ...interface{}) bool {
	if err != nil {
		l.coreIff(2, err, ERROR, format, options...)
		return true
	}
	return false
}

func (l *Logger) Debugf(format string, options ...interface{}) bool {
	return l.Coref(DEBUG, format, options...)
}

func (l *Logger) Secretf(format string, options ...interface{}) bool {
	return l.Coref(SECRET, format, options...)
}

func (l *Logger) Infof(format string, options ...interface{}) bool {
	return l.Coref(INFO, format, options...)
}

func (l *Logger) Warnf(format string, options ...interface{}) bool {
	return l.Coref(WARN, format, options...)
}

func (l *Logger) Errorf(format string, options ...interface{}) bool {
	return l.Coref(ERROR, format, options...)
}

// Little play on words. It's a logged errorf() that returns an error so you can return it AS an error
func (l *Logger) ErrErrf(format string, options ...interface{}) error {
	l.Coref(ERROR, format, options...)
	return fmt.Errorf(format, options...)
}

func (l *Logger) Critf(format string, options ...interface{}) bool {
	return l.Coref(WARN, format, options...)
}
