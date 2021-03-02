# ch-placement

libch-placement is a modular consistent hashing library. It can be used by a
distributed storage system to access multiple hashing algorithms, distance
metrics, and virtual node settings using a consistent API.

## Basic installation

```
./prepare # if cloned from git
./configure --prefix=/desired/installation/path
make -j 3
make install
make check # optional
```

## To enable optional CRUSH support

NOTE: this has only been tested with Ceph version 0.94.3.  The CRUSH API
may be different in other versions of Ceph.

NOTE: The CRUSH API is not normally exposed in the default Ceph header files.
The following instructions explain how to copy the necessary files into the Ceph
installation directory so that CRUSH can be used by an external library.
The following instructions assume that you have compiled Ceph in
a separate subdirectory from the Ceph source code called "build".  If that
is not the case then just remove the "build/" prefix from files described
below.

* compile and install CEPH to $CEPH_PREFIX
  * copy src/crush/{crush|builder|mapper|hash}.h to $CEPH_PREFIX/include
  * copy build/src/acconfig.h to $CEPH_PREFIX/include
  * make directory $CEPH_PREFIX/include/include
  * copy src/include/int_types.h to $CEPH_PREFIX/include/include

* follow basic installation instructions for libch-placement, except add 
  --with-crush=$CEPH_PREFIX to the configure command line

## To enable "vring" bucket support

The vring bucket is an experimental bucket algorith for CRUSH.  To enable it:

* apply the patches/ceph-0.94.3-crush-vring.patch to Ceph
* modify include/ch-placement.h to define CH_ENABLE_CRUSH_VRING
* follow the instructions above to install ch-placement with CRUSH support


