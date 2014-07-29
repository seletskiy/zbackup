package zfs

import (
	"io"
	"strings"

	"github.com/op/go-logging"
	"github.com/theairkit/runcmd"
)

var (
	log = logging.MustGetLogger("zbackup")
)

const (
	FS   = "filesystem"
	SNAP = "snapshot"
)

type Zfs struct {
	runcmd.Runner
}

var std = NewZfs(runcmd.NewLocalRunner())

func NewZfs(nr runcmd.Runner) *Zfs {
	return &Zfs{nr}
}

func CreateSnapshot(fs, snapName string) error {
	return std.CreateSnapshot(fs, snapName)
}

func (this *Zfs) CreateSnapshot(fs, snapName string) error {
	_, err := this.Run("zfs snapshot " + fs + "@" + snapName)
	return err
}

func DestroyFs(fs string) error {
	return std.DestroyFs(fs)
}
func (this *Zfs) DestroyFs(fs string) error {
	_, err := this.Run("zfs destroy -r " + fs)
	return err
}

func ExistFs(fs, fsType string) (bool, error) {
	return std.ExistFs(fs, fsType)
}

func (z *Zfs) ExistFs(fs, fsType string) (bool, error) {
	if _, err := z.ListFs(fs, fsType, false); err != nil {
		if strings.Contains(err.Error(), "dataset does not exist") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func ListFs(fsName, fsType string, recursive bool) ([]string, error) {
	return std.ListFs(fsName, fsType, recursive)
}

func (this *Zfs) ListFs(fsName, fsType string, recursive bool) ([]string, error) {
	fsList := make([]string, 0)
	r := ""
	if recursive {
		r = "-r "
	}
	if strings.Contains(fsName, "*") {
		allFs, err := this.Run("zfs list -H -t " + fsType + " -o name -r")
		if err != nil {
			return fsList, err
		}
		for _, nextFs := range allFs {
			if strings.Contains(nextFs, strings.Trim(fsName, "*")) {
				fsList = append(fsList, nextFs)
			}
		}
		return fsList, err
	}
	return this.Run("zfs list -H -t " + fsType + " -o name " + r + fsName)
}

func RenameFs(oldName, newName string) error {
	return std.RenameFs(oldName, newName)
}
func (this *Zfs) RenameFs(oldName, newName string) error {
	_, err := this.Run("zfs rename " + oldName + " " + newName)
	return err
}

func SendSnapshot(fs, snapCurr, snapNew string, writer io.Writer) error {
	return std.SendSnapshot(fs, snapCurr, snapNew, writer)
}
func (this *Zfs) SendSnapshot(fs, snapCurr, snapNew string, writer io.Writer) error {
	cmd := ""
	if snapNew == "" {
		cmd = "zfs send " + fs + "@" + snapCurr
	} else {
		cmd = "zfs send -i " + fs + "@" + snapCurr + " " + fs + "@" + snapNew
	}
	cmdSend, err := this.Start(cmd)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, cmdSend.Stdout)
	return err
}

func RecvSnapshot(fs, snap string) (*runcmd.Command, error) {
	return std.RecvSnapshot(fs, snap)
}

func (this *Zfs) RecvSnapshot(fs, snap string) (*runcmd.Command, error) {
	return this.Start("zfs recv -F " + fs + "@" + snap)
}
