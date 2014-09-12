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
	User         string   `toml:"user"`
	Host         string   `toml:"host"`
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
	local   string
	remote  string
	expire  string
	lRunner *zfs.Zfs
	rRunner *zfs.Zfs
}

var (
	errUser      = errors.New("user not declared")
	errHost      = errors.New("host not declared")
	errKey       = errors.New("key not declared")
	errBackup    = errors.New("[[backup]] section not declared")
	errFsLocal   = errors.New("[[backup]]: local not declared")
	errFsRemote  = errors.New("[[backup]]: remote not declared")
	warnFsPrefix = "remote_prefix is set; fs with this name on remote will be overwritten"
	warnExpire   = "expire_hours is not set, will not delete old backups"
	warnIoThread = "max_io_threads is not set, or set to '0', setting it to '1'"
	snapExist    = "already exists, wait next minute and run again"
	timeFormat   = "2006-01-02T15:04"
	snapCurr     = "curr"
	snapNew      = "new"
	h, _         = os.Hostname()
)

func loadConfig(path string, c *Config) error {
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

func NewBackuper(c *Config) *Backuper {
	l := zfs.NewZfs(runcmd.NewLocalRunner())
	if l == nil {
		log.Error("cannot create new local runner")
	}
	r := zfs.NewZfs(runcmd.NewRemoteRunner(c.User, c.Host, c.Key))
	if r == nil {
		log.Error("cannot create new remote runner")
	}
	return &Backuper{l, r, c}
}

func (this *Backuper) setupTasks() []BackupTask {
	var bt []BackupTask
	taskid := 0
	for i := range this.c.Backup {
		fsList, err := this.lRunner.ListFs(
			this.c.Backup[i].Local,
			zfs.FS,
			this.c.Backup[i].Recursive,
		)

		if err != nil {
			log.Error(err.Error())
			continue
		}
		for _, local := range fsList {
			remote := remoteName(local, this.c.Backup[i])
			bt = append(bt, BackupTask{
				taskid,
				local,
				remote,
				this.c.Backup[i].Expire,
				this.lRunner,
				this.rRunner,
			})
			taskid++
		}
	}
	return bt
}

func (this *BackupTask) doBackup() error {
	snapshotPostfix := time.Now().Format(timeFormat)
	log.Debug("[%d]: check %s exists", this.id, this.remote+"@"+snapshotPostfix)
	if exist, err := this.rRunner.ExistSnap(this.remote, snapshotPostfix); err != nil || exist {
		if err != nil {
			return err
		}
		return errors.New(this.remote + "@" + snapshotPostfix + " " + snapExist)
	}
	log.Debug("[%d]: check %s exists", this.id, this.local+"@"+snapCurr)
	if exist, err := this.lRunner.ExistSnap(this.local, snapCurr); err != nil || !exist {
		if err != nil {
			return err
		}
		return this.backupHelper("")
	}
	if err := this.backupHelper(snapNew); err != nil {
		return err
	}
	return this.cleanExpired()
}

func (this *BackupTask) backupHelper(snapNew string) error {
	snapshotPostfix := time.Now().Format(timeFormat)
	snap := snapCurr
	if snapNew != "" {
		snap = snapNew
	}
	log.Debug("[%d]: create snapshot: %s...", this.id, snap)
	if err := this.lRunner.CreateSnapshot(this.local, snap); err != nil {
		return err
	}
	log.Debug("[%d]: prepare for recieve snapshot on remote...", this.id)
	cmdRecv, err := this.rRunner.RecvSnapshot(this.remote, snapshotPostfix)
	if err != nil {
		return err
	}
	log.Debug("[%d]: copy snapshot from local to remote...", this.id)
	if err := this.lRunner.SendSnapshot(this.local, snapCurr, snapNew, cmdRecv); err != nil {
		return err
	}
	if err := cmdRecv.Wait(); err != nil {
		return err
	}
	if snapNew != "" {
		log.Debug("[%d]: rotate snapshots (destroy @curr, move @new to @curr)...", this.id)
		if err := this.lRunner.DestroyFs(this.local + "@" + snapCurr); err != nil {
			return err
		}
		if err := this.lRunner.RenameFs(this.local+"@"+snapNew, this.local+"@"+snapCurr); err != nil {
			return err
		}
	}
	return nil
}

func (this *BackupTask) cleanExpired() error {
	log.Debug("[%d]: start cleaning expired snapshot on remote, expire: %s",
		this.id,
		this.expire,
	)

	log.Debug("[%d]: get list remote snapshots...", this.id)
	l, err := this.rRunner.ListFs(this.remote, zfs.SNAP, true)
	if err != nil {
		return err
	}
	if this.expire == "" {
		log.Info("[%d]: %s", this.id, this.expire)
		return nil
	}

	log.Debug("[%d]: determines expired snapshot...", this.id)
	for _, snapshot := range l {
		poolDate, _ := time.ParseInLocation(timeFormat, strings.Split(snapshot, "@")[1], time.Local)
		expire, _ := time.ParseDuration(this.expire)
		if time.Since(poolDate) > expire {
			log.Debug("[%d]: %s will be delete (>%s)", this.id, snapshot, this.expire)
			if err := this.rRunner.DestroyFs(snapshot); err != nil {
				log.Error("[%d]: error destroying %s: %s", this.id, snapshot, err.Error())
				continue
			}
		} else {
			log.Debug("[%d]: %s not exipred, skipping", this.id, snapshot)
		}
	}

	return nil
}

func remoteName(local string, b Backup) string {
	if b.RemotePrefix != "" {
		return b.RemoteRoot + "/" + b.RemotePrefix
	}
	return b.RemotePrefix + "/" + h + "-" + strings.Replace(local, "/", "-", -1)
}
