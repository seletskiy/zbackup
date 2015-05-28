package main

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/theairkit/runcmd"
	"github.com/theairkit/zfs"
)

var (
	errPrefix     = "'remote_prefix' and 'recursive' are mutually exclusive; skip this [[backup]] section"
	errPrefixMask = "'remote_prefix' and 'regexp' are mutually exclusive; skip this [[backup]] section"
	errRegex      = "'regexp' and 'recursive=true' are mutually exclusive; skip this [[backup]] section"
	warnPrefixFs  = "'remote_prefix' set; fs with this name on remote may be overwritten"
	warnExpire    = "expire_hours not set, will not delete old backups"
	warnWaitExist = "already exists, wait next minute and run again"
	timeFormat    = "2006-01-02T15:04"
	snapCurr      = "zbackup_curr"
	snapNew       = "zbackup_new"
	PROPERTY      = "zbackup:"
	hostname, _   = os.Hostname()
)

type Backuper struct {
	config   *Config
	localZfs *zfs.Zfs
}

type BackupTask struct {
	local  bool
	id     int
	src    string
	dst    string
	expire string
	srcZfs *zfs.Zfs
	dstZfs *zfs.Zfs
}

func NewBackuper(c *Config) (*Backuper, error) {
	localZfs, err := zfs.NewZfs(runcmd.NewLocalRunner())
	if err != nil {
		return nil, err
	}
	return &Backuper{c, localZfs}, nil
}

func (backuper *Backuper) setupTasks() ([]BackupTask, error) {
	var localZfs *zfs.Zfs
	var remoteZfs *zfs.Zfs
	var err error

	tasks := make([]BackupTask, 0)
	taskid := 0
	config := backuper.config.Backup

	for _, backup := range config {
		if backup.RemotePrefix != "" && backup.Recursive {
			log.Error("%s: %s", backup.Fs, errPrefix)
			continue
		}
		if backup.Recursive && strings.HasSuffix(backup.Fs, "*") {
			log.Error("%s: %s", backup.Fs, errRegex)
			continue
		}
		if backup.LocalMode == nil {
			*backup.LocalMode = backuper.config.LocalMode
		}
		if *backup.LocalMode == true {
			remoteZfs, err = zfs.NewZfs(runcmd.NewLocalRunner())
		} else {
			remoteZfs, err = zfs.NewZfs(runcmd.NewRemoteKeyAuthRunner(
				backup.Host,
				backup.User,
				backup.Key,
			))
		}
		if err != nil {
			return nil, err
		}

		fsList, err := backuper.localZfs.List(backup.Fs, zfs.FS, backup.Recursive)
		if err != nil {
			log.Error("error get filesystems: %s", err.Error())
			continue
		}
		for _, srcFs := range fsList {
			var dstFs string
			var local bool
			if backuper.config.LocalMode == true {
				local = true
				dstFs = backup.DstPool + "/" + hostname + "-" + strings.Replace(srcFs, "/", "-", -1)
			} else {
				local = false
				dstFs = backup.DstPool + "/" + strings.Replace(srcFs, "/", "-", -1)
			}
			if backup.RemotePrefix != "" {
				dstFs = backup.DstPool + "/" + backup.RemotePrefix
			}
			tasks = append(tasks, BackupTask{
				local,
				taskid,
				srcFs,
				dstFs,
				backup.Expire,
				localZfs,
				remoteZfs,
			})
			taskid++
		}
	}
	return tasks, nil
}

func (task *BackupTask) doBackup() error {
	var err error
	snapPostfix := time.Now().Format(timeFormat)
	id := task.id
	src := task.src
	dst := task.dst

	log.Debug("[%d]: check %s exists", id, dst+"@"+snapPostfix)
	if exist, err := task.dstZfs.ExistSnap(dst, snapPostfix); err != nil || exist {
		if err != nil {
			return err
		}
		return errors.New(dst + "@" + snapPostfix + " " + warnWaitExist)
	}

	log.Debug("[%d]: check %s exists", id, src+"@"+snapCurr)
	if exist, err := task.srcZfs.ExistSnap(src, snapCurr); err != nil || !exist {
		if err != nil {
			return err
		}
		snapNew = ""
	}

	if task.local {
		err = task.doLocalBackup(snapNew)
	} else {
		err = task.doRemoteBackup(snapNew)
	}
	if err != nil {
		return err
	}

	// Cleanup:
	return task.cleanExpired()
}

func (task *BackupTask) doLocalBackup(snapNew string) error {
	snapPostfix := time.Now().Format(timeFormat)
	id := task.id
	src := task.src
	dst := task.dst

	log.Debug("[%d]: create snapshot: %s...", id, snapPostfix)
	if err := task.srcZfs.CreateSnap(src, snapPostfix); err != nil {
		return err
	}

	log.Debug("[%d]: set local %s 'readonly'...", id, dst)
	if err := task.srcZfs.SetProperty(src, "readonly", "on"); err != nil {
		return err
	}

	log.Debug("[%d]: set remote %s 'zbackup:=true'...", id, src+snapPostfix)
	return task.dstZfs.SetProperty(src+"@"+snapPostfix, PROPERTY, "true")
}

func (task *BackupTask) doRemoteBackup(snapNew string) error {
	snapPostfix := time.Now().Format(timeFormat)
	id := task.id
	src := task.src
	dst := task.dst

	snap := snapCurr
	if snapNew != "" {
		snap = snapNew
	}

	log.Debug("[%d]: create snapshot: %s...", id, snap)
	if err := task.srcZfs.CreateSnap(src, snap); err != nil {
		return err
	}

	log.Debug("[%d]: start recieve snapshot on remote...", id)
	cmdRecv, err := task.dstZfs.RecvSnap(dst, snapPostfix)
	if err != nil {
		return err
	}

	log.Debug("[%d]: copy snapshot from local to remote...", id)
	cmdSend, err := task.srcZfs.SendSnap(src, snapCurr, snapNew, cmdRecv)
	if err != nil {
		return err
	}

	if err := cmdSend.Wait(); err != nil {
		return err
	}
	if err := cmdRecv.Wait(); err != nil {
		return err
	}

	if snapNew != "" {
		log.Debug("[%d]: rotate snapshots (destroy @curr, move @new to @curr)...", id)
		if err := task.srcZfs.Destroy(src + "@" + snapCurr); err != nil {
			return err
		}
		if err := task.srcZfs.RenameSnap(src, snapNew, snapCurr); err != nil {
			return err
		}
	}

	log.Debug("[%d]: set remote %s 'readonly'...", id, dst)
	if err := task.dstZfs.SetProperty(dst, "readonly", "on"); err != nil {
		return err
	}

	log.Debug("[%d]: set remote %s 'zbackup:=true'...", id, dst+snapPostfix)
	return task.dstZfs.SetProperty(dst+"@"+snapPostfix, PROPERTY, "true")
}

func (task *BackupTask) cleanExpired() error {
	id := task.id
	dst := task.dst
	expire := task.expire

	log.Debug("[%d]: cleaning expired snapshots, expire: %s", id, expire)
	if expire == "" {
		log.Info("[%d]: expire is not set, exit", id)
		return nil
	}

	recent, err := task.dstZfs.RecentSnap(dst, PROPERTY)
	if err != nil {
		return err
	}

	log.Debug("[%d]: get list remote snapshots...", id)
	list, err := task.dstZfs.ListFsSnap(dst)
	if err != nil {
		return err
	}
	if len(list) == 1 {
		log.Info("[%d] only one snapshot, nothing to delete", id)
		return nil
	}
	for _, snap := range list {
		out, err := task.dstZfs.Property(snap, "zbackup:")
		if err != nil {
			return err
		}
		if out != "true" {
			log.Debug("[%d]: %s is not created by zbackup, skipping", id, snap)
			continue
		}
		if task.expire == "lastone" {
			if snap != recent {
				log.Debug("[%d]: %s will be destroy (not recent)", id, snap)
				if err := task.dstZfs.Destroy(snap); err != nil {
					log.Error("[%d]: error destroying %s: %s", id, snap, err.Error())
				}
			}
			continue
		}

		poolDate, _ := time.ParseInLocation(
			timeFormat,
			strings.Split(snap, "@")[1],
			time.Local,
		)
		expire, _ := time.ParseDuration(expire)
		if time.Since(poolDate) > expire {
			log.Debug("[%d]: %s will be destroy (>%s)", id, snap, expire)

			if err := task.dstZfs.Destroy(snap); err != nil {
				log.Error("[%d]: error destroying %s: %s", id, snap, err.Error())
				continue
			}
		} else {
			log.Debug("[%d]: %s not exipred, skipping", id, snap)
		}
	}
	return nil
}
