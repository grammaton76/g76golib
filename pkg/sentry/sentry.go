package sentry

import (
	"container/list"
	"fmt"
	"github.com/grammaton76/g76golib/pkg/slogger"
	"sync"
	"time"
)

type Sentry struct {
	team         *SentryTeam
	Name         string
	LastCheck    time.Time
	Deadline     time.Time
	ttl          time.Duration
	note         string
	checks       int64
	active       bool
	TripwireFunc func(*Sentry)
}

type sentryreport struct {
	s          *Sentry
	deathcount int64
	expiration time.Time
}

type SentryTeam struct {
	Name          string
	Sentries      map[string]*Sentry
	DefaultTtl    time.Duration
	CheckInterval time.Duration
	Active        bool
	lines         map[time.Duration]chan *sentryreport
	unity         chan *sentryreport
	TripwireFunc  func(*Sentry)
	mutex         sync.Mutex
}

var log *slogger.Logger

func SetLogger(l *slogger.Logger) *slogger.Logger {
	log = l
	return l
}

func (s *Sentry) Counter() int64 {
	return s.checks
}

func (s *Sentry) Notes() string {
	return s.note
}

func (st *SentryTeam) Ensure(name string) *Sentry {
	Sentry, Found := st.Sentries[name]
	if Found {
		return Sentry
	}
	st.NewSentry(name)
	return st.Sentries[name]
}

func (st *SentryTeam) CheckEnsure(name string, format string, opt ...interface{}) *Sentry {
	Sentry, Found := st.Sentries[name]
	if !Found {
		Sentry = st.NewSentry(name)
	}
	Sentry.Checkin(format, opt...)
	return Sentry
}

func (st *SentryTeam) Identifier() string {
	if st.Name != "" {
		return fmt.Sprintf("SentryTeam '%s'", st.Name)
	}
	return "SentryTeam default"
}

func (s *Sentry) Activate() {
	s.active = true
}

func (s *Sentry) Deactivate() {
	s.active = false
}

func (s *Sentry) Identifier() string {
	if s.team.Name == "" {
		return fmt.Sprintf("default sentry '%s'",
			s.Name)
	}
	return fmt.Sprintf("team '%s' sentry '%s'",
		s.team.Name, s.Name)
}

func (st *SentryTeam) NewSentry(Name string, opts ...interface{}) *Sentry {
	s := &Sentry{
		team:   st,
		Name:   Name,
		ttl:    st.DefaultTtl,
		active: true,
	}
	if len(opts) > 0 {
		s.Name = fmt.Sprintf(Name, opts...)
	}
	st.Sentries[s.Name] = s
	log.Debugf("Created sentry '%s'\n", s.Name)
	return s
}

func (s *Sentry) SetTTL(duration time.Duration) *Sentry {
	s.ttl = duration
	return s
}

func (s *Sentry) Update(format string, args ...interface{}) {
	s.note = fmt.Sprintf(format, args...)
	//log.Debugf("Sentry %s update: %s", s.Identifier(), s.note)
}

func (s *Sentry) Checkin(format string, args ...interface{}) {
	channel, found := s.team.lines[s.ttl]
	if !found {
		log.Debugf("No existing channel found for '%s' on team %s; starting.\n",
			s.ttl.String(), s.team.Identifier())
		channel = s.team.startThread(s.ttl)
	}
	s.checks++
	s.Update(format, args...)
	s.LastCheck = time.Now()
	s.Deadline = s.LastCheck.Add(s.ttl)
	log.Debugf("Sentry %s check-in %d revised deadline to %s - '%s'\n",
		s.Identifier(), s.checks, s.Deadline.String(), s.note)
	Report := s.report()
	log.Debugf("Sent %s check-in %d to channel '%s' - %s\n",
		s.Identifier(), Report.deathcount, s.ttl.String(), s.note)
	channel <- Report
}

func (s *Sentry) report() *sentryreport {
	return &sentryreport{
		s:          s,
		deathcount: s.checks,
		expiration: s.Deadline,
	}
}

func (st *SentryTeam) startThread(d time.Duration) chan *sentryreport {
	log.Debugf("Starting channel watch thread on '%s' for duration %s\n",
		st.Identifier(), d.String())
	channel := make(chan *sentryreport)
	st.mutex.Lock()
	st.lines[d] = channel
	st.mutex.Unlock()
	go func(ch chan *sentryreport) {
		queue := list.New()
		go func() {
			for {
				Len := queue.Len()
				if Len == 0 {
					log.Printf("Empty %s queue; sleeping %s before checking again.\n",
						d, time.Duration(d/2))
					time.Sleep(d / 2)
					continue
				}
				Report := queue.Remove(queue.Front()).(*sentryreport)
				if !Report.s.active {
					log.Debugf("Deactivated sentry %s timeline received. Ignoring.\n",
						Report.s.Identifier())
					continue
				}
				if Report.expiration.After(time.Now()) {
					wait := Report.expiration.Sub(time.Now())
					log.Debugf("Received report on channel %s; waiting '%s' to mature.\n",
						d.String(), wait.String())
					time.Sleep(d)
					log.Debugf("Delay for thread '%s' is expired; sentries armed.\n", d.String())
				}
				log.Debugf("Received check-in %d from %s on channel %s: '%s'\n",
					Report.deathcount, Report.s.Identifier(), d.String(), Report.s.note)
				// We ignore all sentry reports which have seen changes
				if Report.deathcount == Report.s.checks {
					log.Debugf("No increment on report '%s'; escalating.\n",
						st.Identifier())
					st.unity <- Report
				} else {
					log.Debugf("%s healthy; required >= %d and got %d.\n",
						Report.s.Identifier(), Report.deathcount, Report.s.checks)
				}
			}
		}()
		for {
			log.Debugf("Waiting for traffic on thread '%s'\n",
				d.String())
			Report := <-ch
			queue.PushBack(Report)
			log.Debugf("Dumped record %s %d into queue.\n", Report.s.Identifier(), Report.deathcount)
		}
	}(st.lines[d])
	return channel
}

func (st *SentryTeam) ActiveStatus() {
	Now := time.Now()
	Caw := fmt.Sprintf("Sentry inventory for '%s' started.\n", Now.String())
	for _, Sentry := range st.Sentries {
		if Sentry.active {
			Caw += fmt.Sprintf("Sentry %s: %s TTL; %d checks, deadline in %s - status '%s'\n",
				Sentry.Name, Sentry.ttl.String(), Sentry.checks, Sentry.Deadline.Sub(Now).String(),
				Sentry.note)
		}
	}
	Caw += fmt.Sprintf("Sentry inventory for '%s' done.\n", Now.String())
	log.Debugf("%s\n", Caw)
}

func NewSentryTeam() *SentryTeam {
	st := &SentryTeam{
		Sentries:   make(map[string]*Sentry),
		DefaultTtl: time.Duration(10) * time.Second,
		Active:     false,
	}
	st.Active = true
	log.Debugf("Started sentry team %s\n", st.Identifier())
	st.lines = make(map[time.Duration]chan *sentryreport)
	st.unity = make(chan *sentryreport)
	go func() {
		for {
			select {
			case Report := <-st.unity:
				log.Debugf("Acting upon check %d from %s\n",
					Report.deathcount, Report.s.Identifier())
				if Report.deathcount == Report.s.checks {
					st.ActiveStatus()
					if Report.s.TripwireFunc != nil {
						Report.s.TripwireFunc(Report.s)
					}
					if st.TripwireFunc != nil {
						st.TripwireFunc(Report.s)
					}
					log.Fatalf("'%s' failed to report in after action %d; last reported activity was '%s', %s ago. Exiting.\n",
						Report.s.Identifier(), Report.deathcount, Report.s.note, Report.s.ttl.String())
				} else {
					log.Debugf("Old deadline processed for '%s' but it's been seen since.\n",
						Report.s.Identifier())
				}
			}
		}
	}()
	return st
}
