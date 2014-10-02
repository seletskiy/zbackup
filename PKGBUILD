# Maintainer: theairkit <theairkit@gmail.com>
pkgname=zbackup-git
pkgver=1
pkgrel=4.5
pkgdesc='zbackup, multithreading zfs backuper written on golang'
arch=("x86_64")
license=('unknown')
provides=('zbackup-git')
backup=('etc/zbackup/zbackup.conf')
md5sums=('SKIP')
_build_branch='dev'
source=(${pkgname}'::git+https://github.com/theairkit/zbackup.git#branch='${_build_branch})
build() {
    cd $srcdir/$pkgname
    go build -o ./zbackup
}
package() {
    install -D       ${srcdir}/${pkgname}/zbackup      ${pkgdir}/usr/bin/zbackup
    install -Dm 0644 ${srcdir}/${pkgname}/zbackup.conf ${pkgdir}/etc/zbackup/zbackup.conf
}
