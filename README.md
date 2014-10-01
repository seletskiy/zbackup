###Now only dev version available: not recommended use it for production backups!

### zbackup

zbackup is a multithreading zfs backuping (via snapshots) tool


###Installation:
####from sources:
```bash
go get github.com/theairkit/runcmd
go get github.com/theairkit/zfs
git clone github.com/theairkit/zbackup .
go install
```

####for archlinux users there are PKGBUILD file:
```bash
git clone github.com/theairkit/zbackup .
makepkg
```

###Description and examples

zbackup can run in two modes:
####command-line mode:
in this mode zbackup perform backup filesystems,
which have a user-defined zfs-property with value 'true'
(colon in property name is part of his name;
this is zfs naming scheme for user-properties)
```
zfs list -H -o name,someprop: zroot/owl                 
zroot/owl   -

zfs set someprop:=true zroot/owl
zfs list -H -o name,someprop: zroot/owl
zroot/owl   true
```

after setting this property to fs, you can run zbackup:
```bash
zbackup -u zbackup: --host 192.186.20.80 --user root --key /root/.ssh/id_rsa
```

####configuration-file mode
in this mode zbackup performs backup filsystem, described in
config file ('-c' key)

configuration file have a TOML format and very simple, e.g.:
```toml
cat /etc/zbackup/zbackup.conf 
user           = "root"
host           = "192.168.20.80:22"
key            = "/root/.ssh/id_rsa"
max_io_threads = 1

[[backup]]
recursive      = true
expire_hours   = "1h"
local          = "zroot/src"
remote_root    = "zroot"
```

after creating configuration file, check config, dry-run and perform zbackup:
```bash
zbackup -c /etc/zbackup/zbackup.conf -t
14:53:45.955870 9892 INFO config ok

zbackup -c /etc/zbackup/zbackup.conf --dry-run
14:54:14.052290 9900 INFO --dry-run set, only show backup tasks:
14:54:14.052345 9900 INFO zroot/src -> 192.168.20.80:22 zroot/owl-zroot-src
14:54:14.052357 9900 INFO zroot/src/blah -> 192.168.20.80:22 zroot/owl-zroot-src-blah
14:54:14.052366 9900 INFO zroot/src/host1 -> 192.168.20.80:22 zroot/owl-zroot-src-host1

zbackup -c /etc/zbackup/zbackup.conf -v debug
14:56:16.782904 9951 INFO [0]: starting backup
... log messages ...
14:56:17.973803 9951 INFO [2]: backup done
```

All command keys (some of them has a default values):
```bash
 zbackup -h
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
  --dry-run       show which fs will backup and exit
  -c configfile   config-based backup (default: /etc/zbackup/zbackup.conf)
  -u zfsproperty  property-based backup (backing up all fs with zfsproperty)
  --host host     set backup host: ${hostname}:${port}
  --user user     set backup user: (default: root)
  --key key       set keyfile: (default: /root/.ssh/id_rsa)
  --iothreads num set max parallel tasks (default: 5)
  --remote fs     set remote fs (default: 'zroot')
  --expire hours  set snapshot expire time in hours: '${n}h' (default: 24h)
```

####Some internals
TODO
