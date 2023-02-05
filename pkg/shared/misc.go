package shared // import "github.com/grammaton76/g76golib/shared"

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/grammaton76/g76golib/pkg/slogger"
	"github.com/kardianos/osext"
	"io/ioutil"
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func SetLogger(l *slogger.Logger) *slogger.Logger {
	log = l
	return l
}

type SafeIntCache struct {
	vars map[string]int
	mux  sync.Mutex
}

func (c *SafeIntCache) Export() (Bob map[string]int) {
	Bob = make(map[string]int)
	for k, v := range c.vars {
		Bob[k] = v
	}
	return Bob
}

func (c *SafeIntCache) Get(key string) (t int) {
	t, _ = c.vars[key]
	return t
}

func (c *SafeIntCache) Check(key string) (int, bool) {
	a, b := c.vars[key]
	return a, b
}

func (c *SafeIntCache) Set(key string, val int) *SafeIntCache {
	c.mux.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.vars[key] = val
	c.mux.Unlock()
	return c
}

func (c *SafeIntCache) SetM(val int, keys ...string) *SafeIntCache {
	c.mux.Lock()
	for _, key := range keys {
		val = c.vars[key]
		c.vars[key] = val
	}
	c.mux.Unlock()
	return c
}

func (c *SafeIntCache) SetPrefix(val int, prefixes ...string) *SafeIntCache {
	c.mux.Lock()
	for _, prefix := range prefixes {
		for v := range c.vars {
			if strings.HasPrefix(v, prefix) {
				c.vars[v] = val
			}
		}
	}
	c.mux.Unlock()
	return c
}

var Shared envLoggingShared

type envLoggingShared struct {
	BootupTime time.Time
	MyMd5sum   string
}

func init() {
	Shared.BootupTime = time.Now()
	log = &slogger.Logger{}
}

func NewDiags() *Diagnostics {
	var this Diagnostics
	syscall.Getrusage(syscall.RUSAGE_SELF, &this.Rusage)
	runtime.ReadMemStats(&this.MemStats)
	Shared.MyMd5sum = GetMyChecksum()
	return &this
}

func ExitIfPidActive(pidFiles ...string) {
	pidFile := EnvParsePath(pidFiles)
	if pidFile == "" && len(pidFiles) > 0 {
		pidFile = pidFiles[0]
	}
	err := writePidFile(pidFile)
	if err != nil {
		log.Infof("Exiting; couldn't lock PID '%s' due to '%s'.\n", pidFile, err)
		os.Exit(3)
	}
}

// Write a pid file, but first make sure it doesn't exist with a running pid.
func writePidFile(pidFile string) error {
	// Read in the pid file as a slice of bytes.
	if piddata, err := ioutil.ReadFile(pidFile); err == nil {
		// Convert the file contents to an integer.
		if pid, err := strconv.Atoi(string(piddata)); err == nil {
			// Look for the pid in the process list.
			if process, err := os.FindProcess(pid); err == nil {
				// Send the process a signal zero kill.
				if err := process.Signal(syscall.Signal(0)); err == nil {
					// We only get an error if the pid isn't running, or it's not ours.
					return fmt.Errorf("pid already running: %d", pid)
				}
			}
		}
	}
	// If we get here, then the pidfile didn't exist,
	// or the pid in it doesn't belong to the user running this app.
	return ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0664)
}

func PidIsRunning(Pid int) bool {
	if process, err := os.FindProcess(Pid); err == nil {
		log.Printf("Process is '%v'\n", process)
		// Send the process a signal zero kill.
		if runtime.GOOS == "windows" {
			log.Printf("Can't send signals on Windows; it is presumed that %d is running.\n", process.Pid)
			return true
		} else {
			if err := process.Signal(syscall.Signal(0)); err == nil {
				// We only get an error if the pid isn't running, or it's not ours.
				log.Printf("pid already running: %d", Pid)
				return true
			} else {
				log.Printf("Got error '%s' on signal call.\n", err)
				return false
			}
		}
	}
	return false
}

func GetParentPid() int {
	return os.Getpid()
}

type Diagnostics struct {
	MemStats runtime.MemStats
	Rusage   syscall.Rusage
}

func (this *Diagnostics) Html() string {
	var Buf string
	Host, _ := os.Hostname()
	uMeg := uint64(1024 * 1024)
	Buf = fmt.Sprintf("<xmp>PID %d on host %s (%s) has been running for %s (since %s)\n\n",
		os.Getpid(), Host, runtime.GOOS, time.Since(Shared.BootupTime), Shared.BootupTime.Local())
	Buf += fmt.Sprintf("We have %d goroutines on this %d CPU host.\n", runtime.NumGoroutine(), runtime.NumCPU())
	Buf += fmt.Sprintf("Executable's checksum is '%s'\n</xmp>", Shared.MyMd5sum)

	Buf += fmt.Sprintf(`<h3>Memory Stats</h3><table border="1">
<tr><th colspan="2">Golang Stats</th></tr>
<tr><td>Alloc</td><td align="right">%v MiB</td></tr>
<tr><td>TotalAlloc</td><td align="right">%v MiB</td></tr>
<tr><td>Sys</td><td align="right">%v MiB</td></tr>
<tr><td>NumGC</td><td align="right">%v</td></tr>
<tr><th colspan="2">OS stats</th></tr>
<tr><td>Max RSS</td><td>%d MiB</td></tr>
</table>
`,
		this.MemStats.Alloc/uMeg, this.MemStats.Alloc/uMeg, this.MemStats.Sys/uMeg, this.MemStats.NumGC, this.Rusage.Maxrss/(1024*1024))
	return Buf
}

func GetMyChecksum() string {
	if Shared.MyMd5sum != "" {
		return Shared.MyMd5sum
	}
	name, err := osext.Executable()
	if err != nil {
		log.Printf("Failed to get executable path for checksum: %s!\n", err)
		return ""
	}
	Shared.MyMd5sum, _ = GetFileChecksum(name)
	log.Printf("Running binary is '%s', with checksum '%s'\n", name, Shared.MyMd5sum)
	return Shared.MyMd5sum
}

func (c *SafeIntCache) Inc(keys ...string) (val int) {
	c.mux.Lock()
	for _, key := range keys {
		val = c.vars[key]
		c.vars[key]++
	}
	// Lock so only one goroutine at a time can access the map c.v.
	c.mux.Unlock()
	return val
}

func NewSafeIntCache() SafeIntCache {
	var Bob SafeIntCache
	Bob.vars = make(map[string]int)
	return Bob
}

type ThreadPurpose struct {
	Purpose map[uint64]string
	timer   time.Duration
	mux     sync.Mutex
	wg      sync.WaitGroup
	defused bool
}

func (t *ThreadPurpose) WgAdd(x int) {
	t.wg.Add(x)
}

func (t *ThreadPurpose) WgWait() {
	t.wg.Wait()
}

func NewThreadPurpose() *ThreadPurpose {
	var t ThreadPurpose
	t.Purpose = make(map[uint64]string)
	return &t
}

func (t *ThreadPurpose) Set(Purpose string) {
	t.mux.Lock()
	t.Purpose[GetGoroutineId()] = Purpose
	t.mux.Unlock()
}

func (t *ThreadPurpose) Done() {
	t.mux.Lock()
	delete(t.Purpose, GetGoroutineId())
	t.wg.Done()
	t.mux.Unlock()
}

func (t *ThreadPurpose) Dump() string {
	var s string
	for k, v := range t.Purpose {
		s = s + fmt.Sprintf("%d: %s\n", k, v)
	}
	return s
}

func (t *ThreadPurpose) DisarmDeadman() {
	log.Debugf("Marking deadman switch disarmed.\n")
	t.defused = true
}

func (t *ThreadPurpose) SetDeadman(Timer time.Duration) {
	//log.Printf("Deadman switch armed.\n")
	time.Sleep(Timer)
	if t.defused != true {
		log.Critf("DEADMAN SWITCH TRIGGERED!\n")
		buf := make([]byte, 1<<16)
		runtime.Stack(buf, true)

		fmt.Printf("%s", buf)
		fmt.Printf("\nRegistered goroutines not reporting done:\n%s", t.Dump())
		os.Exit(100)
	}
}

func GetGoroutineId() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func SearchPath(File string, Paths []string) string {
	for _, v := range Paths {
		Caw := v + "/" + File
		if FileExists(Caw) {
			return Caw
		}
	}
	return ""
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	} else if os.IsPermission(err) {
		return true
	} else if err != nil {
		log.Printf("Error in existence check: %s\n", err)
		return false
	}
	return !info.IsDir()
}

func GetMtime(Path string) (time.Time, error) {
	log.Printf("Checking mtime on '%s'\n", Path)
	fi, err := os.Stat(Path)
	if err != nil {
		return time.Time{}, err
	}
	mtime := fi.ModTime()
	return mtime, nil
}

func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}
	return b, nil
}

func GenerateRandomString(s int) (string, error) {
	b, err := GenerateRandomBytes(s)
	if err != nil {
		return "", err
	}
	str := base64.URLEncoding.EncodeToString(b)
	str = str[0:s]
	return str, err
}

func GenerateRandomUUid() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid, nil
}

func GenerateRandomStringOrDie(s int) string {
	b, err := GenerateRandomString(s)
	if err != nil {
		log.Fatalf("Random string generation error: %s\n", err)
	}
	return b
}

func GetFileChecksum(Filename string) (string, error) {
	dat, err := ioutil.ReadFile(Filename)
	if err != nil {
		return "", fmt.Errorf("file '%s' not loadable in md5", Filename)
	}
	hasher := md5.New()
	hasher.Write(dat)
	return hex.EncodeToString(hasher.Sum(nil)), nil
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

func ReadStdinToEofBytes() []byte {
	var buf []byte
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadBytes('\n')
		buf = append(buf, line...)
		if err != nil {
			return buf
		}
	}
	return nil
}
