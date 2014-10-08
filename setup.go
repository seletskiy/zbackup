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
	errUser      = errors.New("user not declared")
	errHost      = errors.New("host not declared")
	errKey       = errors.New("key not declared")
	errBackup    = errors.New("[[backup]]: section not declared")
	errFsLocal   = errors.New("[[backup]]: local not declared")
	errFsRemote  = errors.New("[[backup]]: remote_root not declared")
	warnIoThread = errors.New("max_io_threads not set, or '0', set to '1'")
)

type Config struct {
	Host         string   `toml:"host"`
	User         string   `toml:"user"`
	Key          string   `toml:"key"`
	MaxIoThreads int      `toml:"max_io_threads"`
	Backup       []Backup `toml:"backup"`
}

type Backup struct {
	Recursive    bool   `toml:"recursive"`
	Expire       string `toml:"expire_hours"`
	Local        string `toml:"local"`
	RemoteRoot   string `toml:"remote_root"`
	RemotePrefix string `toml:"remote_prefix"`
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
	switch {
	case config.User == "":
		return nil, errUser
	case config.Host == "":
		return nil, errHost
	case config.Key == "":
		return nil, errKey
	case len(config.Backup) < 1:
		return nil, errBackup
	case config.MaxIoThreads == 0:
		log.Warning(warnIoThread.Error())
		config.MaxIoThreads = 1
	}
	for i := range config.Backup {
		switch {
		case config.Backup[i].Local == "":
			return nil, errFsLocal
		case config.Backup[i].RemoteRoot == "":
			return nil, errFsRemote
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
				Backup{false, expire, fs, remote, ""},
			)
		}
	}
	return config, nil
}
