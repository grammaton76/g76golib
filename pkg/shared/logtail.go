package shared

import (
	"fmt"
	"github.com/papertrail/go-tail/follower"
	"io"
	"os"
	"time"
)

type logreader func(string, follower.Line) bool

func PrintFromFile(Filename string, line follower.Line) bool {
	fmt.Printf("%s: %s\n", Filename, line.String())
	return true
}

func MonitorLog(Filename string, fn logreader, polldelay int) {
	var WasGone bool = false
	Whence := io.SeekEnd
	for true {
		for !FileExists(Filename) {
			WasGone = true
			//fmt.Printf("File %s does not exist; waiting %d seconds.\n", Filename, polldelay)
			time.Sleep(time.Duration(polldelay) * time.Second)
		}
		if WasGone {
			Whence = io.SeekStart
			log.Printf("File %s finally exists.\n", Filename)
		}

		t, err := follower.New(Filename, follower.Config{
			Whence: Whence,
			Offset: 0,
			Reopen: true,
		})
		if err != nil {
			log.Fatalf("ERR: %s\n", err)
		}
		for line := range t.Lines() {
			fn(Filename, line)
		}
		if t.Err() != nil {
			err = t.Err()
			switch err.(type) {
			case *os.PathError:
				log.Printf("File '%s' was removed; going back to wait loop.\n", Filename)
			default:
				log.Printf("Unknown error related to log '%s'. Regarding this as fatal and exiting.\n", err)
				return
			}
		}
	}
}
