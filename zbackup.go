package main

import (
	"os"
	"strconv"
	"sync"

	"github.com/docopt/docopt-go"
	"github.com/op/go-logging"
)

const version = "1.4.5"

var (
	usage = `
Usage:
  zbackup
  zbackup [-h] [-t] [-p pidfile] [-v loglevel] [-f logfile] [--dry-run]
          [(-c configfile | -u zfsproperty --host host [--user user] [--key key] [--iothreads num] [--remote fs] [--expire hours])]

Options:
  -h              this help
  -t              test configuration and exit
  -p pidfile      set pidfile (default: /var/run/zbackup.pid)
  -v loglevel     set loglevel: info, debug (default: info)
  -f logfile      set logfile (default: stderr)
  --dry-run       show fs will be backup and exit
  -c configfile   config-based backup (default: /etc/zbackup/zbackup.conf)
  -u zfsproperty  property-based backup (backing up all fs with zfs property)
  --host host     set backup host: ${hostname}:${port}
  --user user     set backup user: (default: root)
  --key key       set keyfile: (default: /root/.ssh/id_rsa)
  --iothreads num set max parallel tasks (default: 5)
  --remote fs     set remote fs (default: 'zroot')
  --expire hours  set snapshot expire time in hours or 'lastone': (default: 24h)`

	conffile   = "/etc/zbackup/zbackup.conf"
	pidfile    = "/var/run/zbackup.pid"
	logfile    = os.Stderr
	key        = "/root/.ssh/id_rsa"
	user       = "root"
	remote     = "zroot"
	expire     = "24h"
	iothreads  = 5
	errPidfile = "pidfile already exists: "
	warnLog    = "unknown loglevel, using loglevel: info"
	warnEmpty  = "no backup tasks"
	format     = "%{time:15:04:05.000000} %{pid} %{level:.8s} %{message}"
	log        = logging.MustGetLogger("zbackup")
	c          Config
	err        error
	exitCode   = 0
	arguments  map[string]interface{}
)

func main() {
	arguments, _ = docopt.Parse(usage, nil, true, version, false)

	createPid()
	defer deletePid()

	loglevel := logging.INFO
	logBackend := logging.NewLogBackend(logfile, "", 0)
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
	if arguments["-u"] != nil {
		property := arguments["-u"].(string)
		host := arguments["--host"].(string)
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
		c = Config{
			Host:         host,
			User:         user,
			Key:          key,
			MaxIoThreads: iothreads,
		}
		err = loadConfigFromArgs(&c, property, remote, expire)
	} else {
		if arguments["-c"] != nil {
			conffile = arguments["-c"].(string)
		}
		err = loadConfigFromFile(&c, conffile)
	}
	if err != nil {
		log.Error("error loading config:  %s", err.Error())
		exitCode = 1
		return
	}
	if arguments["-t"].(bool) {
		log.Info("config ok")
		return
	}
	logging.SetLevel(loglevel, log.Module)

	backuper, err := NewBackuper(&c)
	if err != nil {
		log.Error(err.Error())
		exitCode = 1
		return
	}
	backupTasks := backuper.setupTasks()
	if len(backupTasks) == 0 {
		log.Warning(warnEmpty)
		return
	}

	if arguments["--dry-run"].(bool) {
		log.Info("--dry-run set, only show backup tasks:")
		for _, b := range backupTasks {
			log.Info("%s -> %s %s", b.src, backuper.c.Host, b.dst)
		}
	} else {
		wg := sync.WaitGroup{}
		mt := make(chan struct{}, c.MaxIoThreads)
		for i, _ := range backupTasks {
			wg.Add(1)
			mt <- struct{}{}
			go func(i int) {
				log.Info("[%d]: starting backup", i)
				if err := backupTasks[i].doBackup(); err != nil {
					log.Error("[%d]: %s", i, err.Error())
					exitCode = 1
				} else {
					log.Info("[%d]: backup done", i)
				}
				<-mt
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
}
