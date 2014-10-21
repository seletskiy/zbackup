# Maintainer: theairkit <theairkit@gmail.com>
pkgname=zbackup-git
pkgver=1.4.5
pkgrel=1
pkgdesc='multithreading zfs backuper written on golang'
license=('unknown')
depends=('go')
url='https://github.com/theairkit/zbackup'
arch=('x86_64')
backup=('etc/zbackup/zbackup.conf')
branch='dev'
source=("${pkgname}::git+https://github.com/theairkit/zbackup#branch=${branch}")
md5sums=('SKIP')
build(){
    go get -u github.com/BurntSushi/toml
    go get -u github.com/docopt/docopt-go
    go get -u github.com/op/go-logging
    go get -u github.com/theairkit/runcmd
    go get -u github.com/theairkit/zfs
    cd ${srcdir}/${pkgname}
    go build -o zbackup
}
package(){
    install -D -m 755 ${srcdir}/${pkgname}/zbackup      ${pkgdir}/usr/bin/zbackup
    install -D -m 644 ${srcdir}/${pkgname}/zbackup.conf ${pkgdir}/etc/zbackup.conf
}
