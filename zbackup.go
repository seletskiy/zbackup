package main

import (
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/docopt/docopt-go"
	"github.com/op/go-logging"
	"github.com/theairkit/runcmd"
	"github.com/theairkit/zfs"
)

const version = "1.2"

var (
	path       = "/etc/zbackup/zbackup.conf"
	pidfile    = "/var/run/zbackup.pid"
	pidfileErr = "pidfile already exists: "
	format     = "%{time:15:04:05.000000} %{pid} %{level:.8s} %{message}"
	log        = logging.MustGetLogger("zbackup")
)

func main() {
	usage := `
Usage:
  zbackup
  zbackup [-h] [-t] [-c filename] [-p pidfile] [-v loglevel]

Options:
  -h             this help
  -t             test configuration and exit
  -c filename    set configuration file (default: /etc/zbackup/zbackup.conf)
  -p pidfile     set pidfile (default: /var/run/zbackup.pid)
  -v loglevel    set loglevel: normal,debug (default: normal)`

	var c Config
	arguments, _ := docopt.Parse(usage, nil, true, version, false)
	loglevel := logging.INFO
	logBackend := logging.NewLogBackend(os.Stderr, "", 0)
	logging.SetBackend(logBackend)
	logging.SetFormatter(logging.MustStringFormatter(format))
	logging.SetLevel(loglevel, log.Module)

	if arguments["-c"] != nil {
		path = arguments["-c"].(string)
	}
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
			log.Info("unknown loglevel, using loglevel: info")
		}
	}
	logging.SetLevel(loglevel, log.Module)

	backupTasks, err := loadConfig(path, &c)
	if err != nil {
		log.Error("error parsing config: %s", err.Error())
		return
	}
	log.Info("config ok")
	if arguments["-t"].(bool) {
		return
	}

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

	lRunner := zfs.NewZfs(runcmd.NewLocalRunner())
	if lRunner == nil {
		log.Error("cannot create new local runner")
		return
	}
	rRunner := zfs.NewZfs(runcmd.NewRemoteRunner(c.User, c.Host, c.Key))
	if rRunner == nil {
		log.Error("cannot create new remote runner")
		return
	}

	wg := sync.WaitGroup{}
	mt := make(chan struct{}, c.MaxIoThreads)
	for i, _ := range backupTasks {
		wg.Add(1)
		mt <- struct{}{}
		go func(i int) {
			log.Info("[%d]: starting backup", i)
			if err := backup(i, backupTasks[i], lRunner, rRunner); err != nil {
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
