package main

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/theairkit/runcmd"
	"github.com/theairkit/zfs"
)

type Backup struct {
	Recursive   bool   `toml:"recursive"`
	ExpireHours string `toml:"expire_hours"`
	Local       string `toml:"local"`
	Remote      string `toml:"remote"`
}

//_
//in progress
type BackupTask struct {
	Host   string
	User   string
	Key    string
	Local  string
	Remote string
}

func (bt BackupTask) doBackup() error {
	return nil
}

//_

type Config struct {
	User         string   `toml:"user"`
	Host         string   `toml:"host"`
	Key          string   `toml:"key"`
	MaxIoThreads int      `toml:"max_io_threads"`
	Backup       []Backup `toml:"backup"`
}

var (
	userErr      = errors.New("user not declared")
	hostErr      = errors.New("host not declared")
	keyErr       = errors.New("key not declared")
	backupErr    = errors.New("[[backup]] section not declared")
	fsLocalErr   = errors.New("[[backup]]: local not declared")
	fsRemoteErr  = errors.New("[[backup]]: remote not declared")
	snapCurr     = "curr"
	snapNew      = "new"
	snapExist    = "already exists, wait next minute and run again"
	expireWarn   = "[[backup]]: expire_hours is not set (don`t delete old backups), will not clean"
	ioThreadWarn = "'max_io_threads' is not set, or set to '0', setting it to '1'"
	timeFormat   = "2006-01-02T15:04"
)

func loadConfig(path string, config *Config) error {
	if _, err := os.Stat(path); err != nil {
		return err
	}
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return err
	}

	switch {
	case config.User == "":
		return userErr
	case config.Host == "":
		return hostErr
	case config.Key == "":
		return keyErr
	case len(config.Backup) < 1:
		return backupErr
	case config.MaxIoThreads == 0:
		log.Warning(ioThreadWarn)
		config.MaxIoThreads = 1
	}
	for i := range config.Backup {
		switch {
		case config.Backup[i].Local == "":
			return fsLocalErr
		case config.Backup[i].Remote == "":
			return fsRemoteErr
		}
	}

	return nil
}

func backup(gi int, local, remote, expire, user, host, key string) error {
	h, _ := os.Hostname()
	remote = remote + "/" + h + "-" + strings.Replace(local, "/", "-", -1)
	log.Debug("[%d]: create remote runner...", gi)
	sshRunner, err := runcmd.NewRemoteRunner(user, host, key)
	if err != nil {
		return err
	}
	rRunner := zfs.NewZfs(sshRunner)
	log.Debug("[%d]: create local runner...", gi)
	lRunner := zfs.NewZfs(runcmd.NewLocalRunner())
	snapshotPostfix := time.Now().Format(timeFormat)
	log.Debug("[%d]: check %s exists on %s", gi, remote+"@"+snapshotPostfix, host)
	if exist, err := rRunner.ExistSnap(remote, snapshotPostfix); err != nil || exist {
		if err != nil {
			return err
		}
		return errors.New(host + ": " + remote + "@" + snapshotPostfix + " " + snapExist)
	}
	log.Debug("[%d]: check %s exists", gi, local+"@"+snapCurr)
	if exist, err := lRunner.ExistSnap(local, snapCurr); err != nil || !exist {
		if err != nil {
			return err
		}
		return doBackup(local, remote, snapCurr, "", lRunner, rRunner, gi)
	}
	if err := doBackup(local, remote, snapCurr, snapNew, lRunner, rRunner, gi); err != nil {
		return err
	}
	return cleanExpiredSnapshot(rRunner, remote, expire, gi)
}

func cleanExpiredSnapshot(runner *zfs.Zfs, fs, expireHours string, gi int) error {
	log.Debug("[%d]: start cleaning expired snapshot on remote, expire: %s", gi, expireHours)
	log.Debug("[%d]: get list remote snapshots...", gi)
	l, err := runner.ListFs(fs, zfs.SNAP, true)
	if err != nil {
		return err
	}
	if expireHours == "" {
		log.Info("[%d]: %s", gi, expireWarn)
		return nil
	}
	if len(l) > 0 {
		log.Debug("[%d]: determines expired snapshot...", gi)
		for _, snapshot := range l {
			poolDate, _ := time.ParseInLocation(timeFormat, strings.Split(snapshot, "@")[1], time.Local)
			expire, _ := time.ParseDuration(expireHours)
			if time.Since(poolDate) > expire {
				log.Debug("[%d]: %s will be delete (>%s), trying to do it...", gi, snapshot, expireHours)
				if err := runner.DestroyFs(snapshot); err != nil {
					log.Error("[%d]: error destroying %s: %s", gi, snapshot, err.Error())
					continue
				}
			} else {
				log.Debug("[%d]: %s not exipred, skipping", gi, snapshot)
			}
		}
	}
	return nil
}

func doBackup(src, dst, snapCurr, snapNew string, lRunner, rRunner *zfs.Zfs, gi int) error {
	snapshotPostfix := time.Now().Format(timeFormat)
	snap := snapCurr
	if snapNew != "" {
		snap = snapNew
	}
	log.Debug("[%d]: create snapshot: %s...", gi, snap)
	if err := lRunner.CreateSnapshot(src, snap); err != nil {
		return err
	}
	log.Debug("[%d]: prepare for recieve snapshot on remote...", gi)
	cmdRecv, err := rRunner.RecvSnapshot(dst, snapshotPostfix)
	if err != nil {
		return err
	}
	log.Debug("[%d]: starting copy snapshot from local to remote...", gi)
	if err := lRunner.SendSnapshot(src, snapCurr, snapNew, cmdRecv.Stdin); err != nil {
		return err
	}
	if err := rRunner.WaitCmd(); err != nil {
		return err
	}
	if snapNew != "" {
		log.Debug("[%d]: start rotate snapshot on local (destroy @curr, move @new to @curr)...", gi)
		if err := lRunner.DestroyFs(src + "@" + snapCurr); err != nil {
			return err
		}
		if err := lRunner.RenameFs(src+"@"+snapNew, src+"@"+snapCurr); err != nil {
			return err
		}
	}
	return nil
}
