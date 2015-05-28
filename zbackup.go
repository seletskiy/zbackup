package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/docopt/docopt-go"
	"github.com/op/go-logging"
)

var (
	version = "1.4.6"

	usage = `
Usage:
	zbackup -h
	zbackup --version
	zbackup [-c config]    [-t] [--dry-run] [-p pidfile] [-v loglevel] [-f logfile]
	zbackup -u zfsproperty [-t] [--dry-run] [-p pidfile] [-v loglevel] [-f logfile]
	--host host [--user user] [--key key] [--iothreads num] [--remote fs] [--expire hours]

Options:
	-h               this help
	--version        show version and exit
	-c config        configuration-based backup [default: /etc/zbackup/zbackup.conf]
	-t               test configuration and exit
	--dry-run        show fs will be backup and exit
	-p pidfile       set pidfile [default: /var/run/zbackup.pid]
	-v loglevel      set loglevel: info, debug [default: info]
	-f logfile       set logfile [default: stderr]
	-u zfsproperty   property-based backup
	--host host      set backup host ${hostname}:${port}
	--user user      set backup user [default: root]
	--key key        set keyfile [default: /root/.ssh/id_rsa]
	--iothreads num  set max parallel tasks [default: 1]
	--remote fs      set remote root fs [default: 'zroot']
	--expire hours   set expire time in hours or 'lastone' [default: 24h]`

	err       error
	config    *Config
	loglevel  logging.Level
	log       = logging.MustGetLogger("zbackup")
	logFormat = "%{time:15:04:05.000000} %{pid} %{level:.8s} %{message}"
	warnEmpty = "no backup tasks"

	exitCode = 0
)

func main() {
	// Parse command-line keys:
	arguments, _ := docopt.Parse(usage, nil, true, version, false)

	// Handle pidfile:
	pidfile := arguments["-p"].(string)
	if err := createPidfile(pidfile); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	defer deletePidfile(pidfile)

	// Setup logging:
	switch arguments["-v"].(string) {
	case "info":
		loglevel = logging.INFO
	case "debug":
		loglevel = logging.DEBUG
	default:
		fmt.Fprintln(os.Stderr, "unknown log level")
		exitCode = 1
		return
	}
	logfile, err := openLogfile(arguments["-f"].(string))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		exitCode = 1
		return
	}
	defer closeLogfile(logfile)
	setupLogger(loglevel, logfile, logFormat)

	// Load config:
	// property-based:
	if arguments["-u"] != nil {
		maxio, err := strconv.Atoi(arguments["--iothreads"].(string))
		if err != nil {
			log.Error("%s", err.Error())
			exitCode = 1
			return
		}
		config, err = loadConfigFromArgs(
			arguments["-u"].(string),
			arguments["--remote"].(string),
			arguments["--expire"].(string),
			arguments["--host"].(string),
			arguments["--user"].(string),
			arguments["--key"].(string),
			maxio,
		)
	} else { // configuration based, arguments["-c"] != nil
		config, err = loadConfigFromFile(arguments["-c"].(string))
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
	// Setup backup tasks:
	backuper, err := NewBackuper(config)
	if err != nil {
		log.Error(err.Error())
		exitCode = 1
		return
	}
	fmt.Println("zbackup-blah1")
	backupTasks, err := backuper.setupTasks()
	if err != nil {
		log.Error(err.Error())
	}
	fmt.Println("zbackup-blah2")
	if len(backupTasks) == 0 {
		fmt.Println("zbackup-blah3")
		log.Warning(warnEmpty)
		return
	}
	fmt.Println("zbackup-blah4")
	// Perform backup or dry-run:
	wg := sync.WaitGroup{}
	mt := make(chan struct{}, config.Threads)
	for i, _ := range backupTasks {
		if arguments["--dry-run"].(bool) {
			log.Info("[%d]: %s -> %s %s",
				i,
				backupTasks[i].src,
				backuper.config.Host,
				backupTasks[i].dst,
			)
			continue
		}
		wg.Add(1)
		mt <- struct{}{}
		go func(id int) {
			log.Info("[%d]: starting backup", id)
			if err := backupTasks[id].doBackup(); err != nil {
				log.Error("[%d]: %s", id, err.Error())
				exitCode = 1
			} else {
				log.Info("[%d]: backup done", id)
			}
			<-mt
			wg.Done()
		}(i)
	}
	wg.Wait()
}
