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

type Config struct {
	User         string   `toml:"user"`
	Host         string   `toml:"host"`
	Key          string   `toml:"key"`
	MaxIoThreads int      `toml:"max_io_threads"`
	Backup       []Backup `toml:"backup"`
}

type BackupTask struct {
	local  string
	remote string
	expire string
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
	h, _         = os.Hostname()
)

func loadConfig(path string, c *Config) ([]BackupTask, error) {
	var bt []BackupTask
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	if _, err := toml.DecodeFile(path, c); err != nil {
		return nil, err
	}

	switch {
	case c.User == "":
		return nil, userErr
	case c.Host == "":
		return nil, hostErr
	case c.Key == "":
		return nil, keyErr
	case len(c.Backup) < 1:
		return nil, backupErr
	case c.MaxIoThreads == 0:
		log.Warning(ioThreadWarn)
		c.MaxIoThreads = 1
	}
	for i := range c.Backup {
		switch {
		case c.Backup[i].Local == "":
			return nil, fsLocalErr
		case c.Backup[i].Remote == "":
			return nil, fsRemoteErr
		}
	}

	lRunner := zfs.NewZfs(runcmd.NewLocalRunner())
	for i := range c.Backup {
		fsList, err := lRunner.ListFs(c.Backup[i].Local, zfs.FS, c.Backup[i].Recursive)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		for _, fs := range fsList {
			bt = append(bt, BackupTask{fs, c.Backup[i].Remote, c.Backup[i].ExpireHours})
		}
	}
	return bt, nil
}

func backup(i int, bt BackupTask, lRunner, rRunner *zfs.Zfs) error {
	remote := bt.remote + "/" + h + "-" + strings.Replace(bt.local, "/", "-", -1)
	snapshotPostfix := time.Now().Format(timeFormat)
	log.Debug("[%d]: check %s exists", i, remote+"@"+snapshotPostfix)
	if exist, err := rRunner.ExistSnap(remote, snapshotPostfix); err != nil || exist {
		if err != nil {
			return err
		}
		return errors.New(remote + "@" + snapshotPostfix + " " + snapExist)
	}
	log.Debug("[%d]: check %s exists", i, bt.local+"@"+snapCurr)
	if exist, err := lRunner.ExistSnap(bt.local, snapCurr); err != nil || !exist {
		if err != nil {
			return err
		}
		return doBackup(i, bt.local, remote, snapCurr, "", lRunner, rRunner)
	}
	if err := doBackup(i, bt.local, remote, snapCurr, snapNew, lRunner, rRunner); err != nil {
		return err
	}
	return cleanExpiredSnapshot(i, rRunner, remote, bt.expire)
}

func cleanExpiredSnapshot(i int, runner *zfs.Zfs, fs, expireHours string) error {
	log.Debug("[%d]: start cleaning expired snapshot on remote, expire: %s", i, expireHours)
	log.Debug("[%d]: get list remote snapshots...", i)
	l, err := runner.ListFs(fs, zfs.SNAP, true)
	if err != nil {
		return err
	}
	if expireHours == "" {
		log.Info("[%d]: %s", i, expireWarn)
		return nil
	}

	log.Debug("[%d]: determines expired snapshot...", i)
	for _, snapshot := range l {
		poolDate, _ := time.ParseInLocation(timeFormat, strings.Split(snapshot, "@")[1], time.Local)
		expire, _ := time.ParseDuration(expireHours)
		if time.Since(poolDate) > expire {
			log.Debug("[%d]: %s will be delete (>%s)", i, snapshot, expireHours)
			if err := runner.DestroyFs(snapshot); err != nil {
				log.Error("[%d]: error destroying %s: %s", i, snapshot, err.Error())
				continue
			}
		} else {
			log.Debug("[%d]: %s not exipred, skipping", i, snapshot)
		}
	}

	return nil
}

func doBackup(i int, src, dst, snapCurr, snapNew string, lRunner, rRunner *zfs.Zfs) error {
	snapshotPostfix := time.Now().Format(timeFormat)
	snap := snapCurr
	if snapNew != "" {
		snap = snapNew
	}
	log.Debug("[%d]: create snapshot: %s...", i, snap)
	if err := lRunner.CreateSnapshot(src, snap); err != nil {
		return err
	}
	log.Debug("[%d]: prepare for recieve snapshot on remote...", i)
	cmdRecv, err := rRunner.RecvSnapshot(dst, snapshotPostfix)
	if err != nil {
		return err
	}
	log.Debug("[%d]: starting copy snapshot from local to remote...", i)
	if err := lRunner.SendSnapshot(src, snapCurr, snapNew, cmdRecv); err != nil {
		return err
	}
	if err := cmdRecv.Wait(); err != nil {
		return err
	}
	if snapNew != "" {
		log.Debug("[%d]: start rotate snapshot on local (destroy @curr, move @new to @curr)...", i)
		if err := lRunner.DestroyFs(src + "@" + snapCurr); err != nil {
			return err
		}
		if err := lRunner.RenameFs(src+"@"+snapNew, src+"@"+snapCurr); err != nil {
			return err
		}
	}
	return nil
}
