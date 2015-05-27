package main

import (
	"errors"
	"io"
	"os"
	"strconv"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/op/go-logging"
	"github.com/theairkit/runcmd"
	"github.com/theairkit/zfs"
)

var (
	errNoUser    = errors.New("'user' not declared")
	errNoHost    = errors.New("'host' not declared")
	errNoKey     = errors.New("'key' not declared")
	errUser      = errors.New("'user' and 'localmode' are mutually exclusive")
	errHost      = errors.New("'host' and 'localmode' are mutually exclusive")
	errKey       = errors.New("'key' and 'localmode' are mutually exclusive")
	errNoBackup  = errors.New("[[backup]]: section not declared")
	errNoFs      = errors.New("[[backup]]: fs not declared")
	errNoDstPool = errors.New("[[backup]]: dst_pool not declared")
	warnNoUser   = errors.New("'user' not declared, skip this [[backup]] section")
	warnNoHost   = errors.New("'host' not declared, skip this [[backup]] section")
	warnNoKey    = errors.New("'key' not declared, skip this [[backup]] section")
	warnUser     = errors.New("'user' and localmode are mutually exclusive, skip this [[backup]] section")
	warnHost     = errors.New("'host' and localmode are mutually exclusive, skip this [[backup]] section")
	warnKey      = errors.New("'key' and localmode are mutually exclusive, skip this [[backup]] section")

	warnThread = errors.New("'threads' less than 1, set to '1'")
)

type Config struct {
	Host      string   `toml:"host"`
	User      string   `toml:"user"`
	Key       string   `toml:"key"`
	Threads   int      `toml:"threads"`
	LocalMode bool     `toml:"localmode"`
	Backup    []Backup `toml:"backup"`
}

type Backup struct {
	Host         string `toml:"host"`
	User         string `toml:"user"`
	Key          string `toml:"key"`
	Recursive    bool   `toml:"recursive"`
	Expire       string `toml:"expire_hours"`
	Fs           string `toml:"fs"`
	DstPool      string `toml:"dst_pool"`
	RemotePrefix string `toml:"remote_prefix"`
	LocalMode    *bool  `toml:"localmode"`
}

func createPidfile(filename string) error {
	if _, err := os.Stat(filename); err == nil {
		return err
	}
	pidfile, err := os.Create(filename)
	if err != nil {
		return err
	}
	if _, err := pidfile.WriteString(
		strconv.Itoa(syscall.Getpid()),
	); err != nil {
		return err
	}
	return nil
}

func deletePidfile(filename string) {
	if err := os.Remove(filename); err != nil {
		log.Error(err.Error())
		exitCode = 1
	}
	os.Exit(exitCode)
}

func openLogfile(filename string) (*os.File, error) {
	if filename == "stderr" {
		return os.Stderr, nil
	}

	fd, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}
	return fd, nil
}

func closeLogfile(file *os.File) error {
	return file.Close()
}

func setupLogger(level logging.Level, writer io.Writer, format string) {
	logBackend := logging.NewLogBackend(writer, "", 0)
	logging.SetBackend(logBackend)
	logging.SetFormatter(logging.MustStringFormatter(logFormat))
	logging.SetLevel(level, log.Module)
}

func loadConfigFromFile(filename string) (*Config, error) {
	if _, err := os.Stat(filename); err != nil {
		return nil, err
	}

	config := Config{}
	if _, err := toml.DecodeFile(filename, &config); err != nil {
		return nil, err
	}

	if config.LocalMode {
		switch {
		case config.User != "":
			return nil, errUser
		case config.Host != "":
			return nil, errHost
		case config.Key != "":
			return nil, errKey
		}
	} else {
		switch {
		case config.User == "":
			return nil, errNoUser
		case config.Host == "":
			return nil, errNoHost
		case config.Key == "":
			return nil, errNoKey
		}
	}
	if len(config.Backup) < 1 {
		return nil, errNoBackup
	}
	if config.Threads < 1 {
		log.Warning(warnThread.Error())
		config.Threads = 1
	}
	for i := range config.Backup {
		if config.Backup[i].LocalMode != nil {
			if *config.Backup[i].LocalMode == true {
				switch {
				case config.Backup[i].User == "":
					log.Warning(warnNoUser.Error())
					continue
				case config.Backup[i].Host == "":
					log.Warning(warnNoHost.Error())
					continue
				case config.Backup[i].Key == "":
					log.Warning(warnNoKey.Error())
					continue
				}
			} else {
				switch {
				case config.Backup[i].User != "":
					log.Warning(warnUser.Error())
					continue
				case config.Backup[i].Host != "":
					log.Warning(warnHost.Error())
					continue
				case config.Backup[i].Key != "":
					log.Warning(warnKey.Error())
					continue
				}
			}
		}
		switch {
		case config.Backup[i].Fs == "":
			return nil, errNoFs
		case config.Backup[i].DstPool == "":
			return nil, errNoDstPool
		}
	}
	return &config, nil
}

func loadConfigFromArgs(
	property,
	remote,
	expire,
	host,
	user,
	key string,
	maxio int,
) (*Config, error) {
	config := new(Config)
	config.Backup = make([]Backup, 0)
	config.Host = host
	config.User = user
	config.Key = key
	config.Threads = maxio

	srcZfs, err := zfs.NewZfs(runcmd.NewLocalRunner())
	if err != nil {
		return nil, err
	}
	list, err := srcZfs.List("", zfs.FS, true)
	if err != nil {
		return nil, err
	}
	for _, fs := range list {
		out, err := srcZfs.Property(fs, property)
		if err != nil {
			return nil, err
		}
		if out == "true" {
			config.Backup = append(
				config.Backup,
				Backup{"", "", "", false, expire, fs, remote, "", nil},
			)
		}
	}
	return config, nil
}
