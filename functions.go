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
	Recursive    bool   `toml:"recursive"`
	Expire       string `toml:"expire_hours"`
	Local        string `toml:"local"`
	RemoteRoot   string `toml:"remote_root"`
	RemotePrefix string `toml:"remote_prefix"`
}

type Config struct {
	Host         string   `toml:"host"`
	User         string   `toml:"user"`
	Key          string   `toml:"key"`
	MaxIoThreads int      `toml:"max_io_threads"`
	Backup       []Backup `toml:"backup"`
}

type Backuper struct {
	lRunner *zfs.Zfs
	rRunner *zfs.Zfs
	c       *Config
}

type BackupTask struct {
	id      int
	src     string
	dst     string
	expire  string
	lRunner *zfs.Zfs
	rRunner *zfs.Zfs
}

var (
	errUser       = errors.New("user not declared")
	errHost       = errors.New("host not declared")
	errKey        = errors.New("key not declared")
	errBackup     = errors.New("[[backup]]: section not declared")
	errFsLocal    = errors.New("[[backup]]: local not declared")
	errFsRemote   = errors.New("[[backup]]: remote_root not declared")
	errPrefix     = "'remote_prefix' and 'recursive' are mutually exclusive; skip this [[backup]] section"
	errPrefixMask = "'remote_prefix' and 'regexp' are mutually exclusive; skip this [[backup]] section"
	warnFsPrefix  = "'remote_prefix' set; fs with this name on remote may be overwritten"
	warnExpire    = "expire_hours not set, will not delete old backups"
	warnIoThread  = "max_io_threads not set, or set to '0', setting it to '1'"
	snapExist     = "already exists, wait next minute and run again"
	timeFormat    = "2006-01-02T15:04"
	snapCurr      = "zbackup_curr"
	snapNew       = "zbackup_new"
	h, _          = os.Hostname()
	PROPERTY      = "zbackup:"
)

func loadConfigFromFile(c *Config, path string) error {
	if _, err := os.Stat(path); err != nil {
		return err
	}
	if _, err := toml.DecodeFile(path, c); err != nil {
		return err
	}
	switch {
	case c.User == "":
		return errUser
	case c.Host == "":
		return errHost
	case c.Key == "":
		return errKey
	case len(c.Backup) < 1:
		return errBackup
	case c.MaxIoThreads == 0:
		log.Warning(warnIoThread)
		c.MaxIoThreads = 1
	}
	for i := range c.Backup {
		switch {
		case c.Backup[i].Local == "":
			return errFsLocal
		case c.Backup[i].RemoteRoot == "":
			return errFsRemote
		case c.Backup[i].RemotePrefix != "":
			log.Warning(warnFsPrefix)
		}
	}
	return nil
}

func loadConfigFromArgs(c *Config, property, remote, expire string) error {
	c.Backup = make([]Backup, 0)
	lRunner, err := zfs.NewZfs(runcmd.NewLocalRunner())
	if err != nil {
		return err
	}
	fsList, err := lRunner.ListFs("", zfs.FS, "", true)
	if err != nil {
		return err
	}
	for i, fs := range fsList {
		out, err := lRunner.Property(fs, property)
		if err != nil {
			return err
		}
		if out == "true" {
			c.Backup[i] = Backup{false, expire, fs, remote, ""}
		}
	}
	return nil
}

func NewBackuper(c *Config) (*Backuper, error) {
	lRunner, err := zfs.NewZfs(runcmd.NewLocalRunner())
	if err != nil {
		return nil, err
	}
	rRunner, err := zfs.NewZfs(runcmd.NewRemoteKeyAuthRunner(c.User, c.Host, c.Key))
	if err != nil {
		return nil, err
	}
	return &Backuper{lRunner, rRunner, c}, nil
}

func (this *Backuper) setupTasks() []BackupTask {
	var bt []BackupTask
	taskid := 0
	c := this.c.Backup

	for i := range this.c.Backup {
		switch {
		case c[i].RemotePrefix != "" && c[i].Recursive:
			log.Error("%s: %s", c[i].Local, errPrefix)
			continue
		case c[i].RemotePrefix != "" && strings.Contains(c[i].Local, "*"):
			log.Error("%s: %s", c[i].Local, errPrefixMask)
			continue
		}

		fsList, err := this.lRunner.ListFs(c[i].Local, zfs.FS, "", c[i].Recursive)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		for _, src := range fsList {
			dst := c[i].RemoteRoot + "/" + h + "-" + strings.Replace(src, "/", "-", -1)
			if c[i].RemotePrefix != "" {
				dst = c[i].RemoteRoot + "/" + c[i].RemotePrefix
			}
			bt = append(bt, BackupTask{
				taskid,
				src,
				dst,
				c[i].Expire,
				this.lRunner,
				this.rRunner,
			})
			taskid++
		}
	}
	return bt
}

func (this *BackupTask) doBackup() error {
	snapPostfix := time.Now().Format(timeFormat)
	id := this.id
	src := this.src
	dst := this.dst

	log.Debug("[%d]: check %s exists", id, dst+"@"+snapPostfix)
	if exist, err := this.rRunner.ExistSnap(dst, snapPostfix); err != nil || exist {
		if err != nil {
			return err
		}
		return errors.New(dst + "@" + snapPostfix + " " + snapExist)
	}
	log.Debug("[%d]: check %s exists", id, src+"@"+snapCurr)
	if exist, err := this.lRunner.ExistSnap(src, snapCurr); err != nil || !exist {
		if err != nil {
			return err
		}
		snapNew = ""
	}
	if err := this.backupHelper(snapNew); err != nil {
		return err
	}

	return this.cleanExpired()
}

func (this *BackupTask) backupHelper(snapNew string) error {
	snapPostfix := time.Now().Format(timeFormat)
	id := this.id
	src := this.src
	dst := this.dst

	snap := snapCurr
	if snapNew != "" {
		snap = snapNew
	}

	log.Debug("[%d]: create snapshot: %s...", id, snap)
	if err := this.lRunner.CreateSnap(src, snap); err != nil {
		return err
	}

	log.Debug("[%d]: start recieve snapshot on remote...", id)
	cmdRecv, err := this.rRunner.RecvSnap(dst, snapPostfix)
	if err != nil {
		return err
	}

	log.Debug("[%d]: copy snapshot from local to remote...", this.id)
	if err := this.lRunner.SendSnap(src, snapCurr, snapNew, cmdRecv); err != nil {
		return err
	}
	if err := cmdRecv.Wait(); err != nil {
		return err
	}

	if snapNew != "" {
		log.Debug("[%d]: rotate snapshots (destroy @curr, move @new to @curr)...", id)
		if err := this.lRunner.DestroyFs(src + "@" + snapCurr); err != nil {
			return err
		}
		if err := this.lRunner.RenameFs(src+"@"+snapNew, src+"@"+snapCurr); err != nil {
			return err
		}
	}

	log.Debug("[%d]: set remote %s 'readonly'...", this.id, dst)
	if err := this.rRunner.SetProperty(dst, "readonly", "on"); err != nil {
		return err
	}

	log.Debug("[%d]: set remote %s 'zbackup:=true'...", this.id, dst+snapPostfix)
	return this.rRunner.SetProperty(dst+"@"+snapPostfix, PROPERTY, "true")
}

func (this *BackupTask) cleanExpired() error {
	id := this.id
	dst := this.dst
	expire := this.expire

	log.Debug("[%d]: cleaning expired snapshots, expire: %s", id, expire)
	if expire == "" {
		log.Info("[%d]: expire is not set, exit", id)
		return nil
	}

	recent, err := this.rRunner.RecentSnap(dst, PROPERTY)
	if err != nil {
		return err
	}

	log.Debug("[%d]: get list remote snapshots...", id)
	snapList, err := this.rRunner.ListFs(dst, zfs.SNAP, PROPERTY, true)
	if err != nil {
		return err
	}
	if len(snapList) == 1 {
		log.Info("[%d] only one snapshot, nothing to delete", id)
		return nil
	}
	for _, snap := range snapList {
		out, err := this.rRunner.Property(snap, "zbackup:")
		if err != nil {
			return err
		}
		if out != "true" {
			log.Debug("[%d]: %s is not created by zbackup, skipping", this.id, snap)
			continue
		}
		if this.expire == "lastone" {
			if snap != recent {
				log.Debug("[%d]: %s will be destroy (not recent)", id, snap)
				if err := this.rRunner.DestroyFs(snap); err != nil {
					log.Error("[%d]: error destroying %s: %s", id, snap, err.Error())
				}
			}
			continue
		}

		poolDate, _ := time.ParseInLocation(timeFormat, strings.Split(snap, "@")[1], time.Local)
		expire, _ := time.ParseDuration(expire)
		if time.Since(poolDate) > expire {
			log.Debug("[%d]: %s will be destroy (>%s)", id, snap, expire)

			if err := this.rRunner.DestroyFs(snap); err != nil {
				log.Error("[%d]: error destroying %s: %s", id, snap, err.Error())
				continue
			}
		} else {
			log.Debug("[%d]: %s not exipred, skipping", id, snap)
		}
	}
	return nil
}
