package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/docopt/docopt-go"
	"github.com/op/go-logging"
)

const version = "1.2"

var (
	path       = "/etc/zbackup/zbackup.conf"
	pidfile    = "/var/run/zbackup.pid"
	errPidfile = "pidfile already exists: "
	warnLog    = "unknown loglevel, using loglevel: info"
	warnEmpty  = "no backup tasks"
	format     = "%{time:15:04:05.000000} %{pid} %{level:.8s} %{message}"
	log        = logging.MustGetLogger("zbackup")
)

func main() {
	usage := `
Usage:
  zbackup
  zbackup [-h] [-t] [-p pidfile] [-v loglevel]
          (-c configfile | -u zfsproperty --host host [--user user] [--key key] [--iothreads num] [--remote fs] [--expire hours])

Options:
  -h              this help
  -t              test configuration and exit
  -p pidfile      set pidfile (default: /var/run/zbackup.pid)
  -v loglevel     set loglevel: info,debug (default: info)
  -c configfile   configuration-based backup (default: /etc/zbackup/zbackup.conf)
  -u zfsproperty  property-based backup, performs backup for fs, having this property
  --host host     set backup host: ${hostname}:${port}
  --user user     set backup user: (root by default)
  --key key       set keyfile: (/root/.ssh/id_rsa by default)
  --iothreads num set iothreads (5 by default)
  --remote fs     set remote fs ('zroot' by default)
  --expire hours  set snapshot expire time in hours: '${n}h' (24h by default)`

	var c Config
	arguments, _ := docopt.Parse(usage, nil, true, version, false)
	loglevel := logging.INFO
	logBackend := logging.NewLogBackend(os.Stderr, "", 0)
	logging.SetBackend(logBackend)
	logging.SetFormatter(logging.MustStringFormatter(format))
	logging.SetLevel(loglevel, log.Module)

	if arguments["-p"] != nil {
		pidfile = arguments["-p"].(string)
	}
	if arguments["-v"] != nil {
		switch arguments["-v"].(string) {
		case "info":
			loglevel = logging.INFO
		case "debug":
			loglevel = logging.DEBUG
		default:
			log.Info(warnLog)
		}
	}
	if arguments["-c"] != nil {
		path = arguments["-c"].(string)
		if err := loadConfigFromFile(&c, path); err != nil {
			log.Error("error loading config:  %s", err.Error())
			return
		}
	}
	if arguments["-u"] != nil {
		property := arguments["-u"].(string)
		host := arguments["--host"].(string)
		user := "root"
		key := "/root/.ssh/id_rsa"
		iothreads := 5
		remote := "zroot"
		expire := "24h"
		if arguments["--user"] != nil {
			user = arguments["--user"].(string)
		}
		if arguments["--key"] != nil {
			key = arguments["--key"].(string)
		}
		if arguments["--iothreads"] != nil {
			iothreads, _ = strconv.Atoi(arguments["--iothreads"].(string))
		}
		if arguments["--remote"] != nil {
			remote = arguments["--remote"].(string)
		}
		if arguments["--expire"] != nil {
			expire = arguments["--expire"].(string)
		}
		c = Config{Host: host, User: user, Key: key, MaxIoThreads: iothreads}
		if err := loadConfigFromArgs(&c, property, remote, expire); err != nil {
			log.Error("error loading config:  %s", err.Error())
			return
		}

	}
	logging.SetLevel(loglevel, log.Module)

	if _, err := os.Stat(pidfile); err == nil {
		log.Error("cannot run: %s already exists", pidfile)
		return
	}
	pid, err := os.Create(pidfile)
	if err != nil {
		log.Error(err.Error())
		return
	}
	defer func() {
		if err := os.Remove(pidfile); err != nil {
			log.Error(err.Error())
		}
	}()
	pid.WriteString(strconv.Itoa(syscall.Getpid()))

	b, err := NewBackuper(&c)
	if err != nil {
		log.Error(err.Error())
		return
	}

	backupTasks := b.setupTasks()
	if len(backupTasks) == 0 {
		log.Warning(warnEmpty)
		return
	}

	//_
	// Debug:
	fmt.Println(c.MaxIoThreads)
	for _, bt := range backupTasks {
		fmt.Println(bt)
	}
	return
	//__

	wg := sync.WaitGroup{}
	mt := make(chan struct{}, c.MaxIoThreads)
	for i, _ := range backupTasks {
		wg.Add(1)
		mt <- struct{}{}
		go func(i int) {
			log.Info("[%d]: starting backup", i)
			if err := backupTasks[i].doBackup(); err != nil {
				log.Error("[%d]: %s", i, err.Error())
			} else {
				log.Info("[%d]: backup done", i)
			}
			<-mt
			wg.Done()
		}(i)
	}
	wg.Wait()
}
