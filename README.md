tcl-leveldb
=====

[LevelDB](https://github.com/google/leveldb) is a fast key-value storage
library written at Google that provides an ordered mapping from string keys
to string values.

This extension is the Tcl interface to the LevelDB.
Now tcl-leveldb at an early development stage (or a prototype).


License
=====

LevelDB is Licensed under New BSD License.

tcl-leveldb is Licensed under MIT License.


UNIX BUILD
=====

I only test on openSUSE LEAP 42.3 and Ubuntu 14.04.

openSUSE LEAP 42.3 install leveldb development files:

    sudo zypper install leveldb-devel

Ubuntu 14.04 install leveldb development files:

    sudo apt-get install libleveldb-dev


After install LevelDB shared library, you can build this extension.

Building under most UNIX systems is easy, just run the configure script
and then run make. For more information about the build process, see
the tcl/unix/README file in the Tcl src dist. The following minimal
example will install the extension in the /opt/tcl directory.

    $ export CC=g++
    $ cd tcl-leveldb
    $ ./configure --prefix=/opt/tcl
    $ make
    $ make install
	
If you need setup directory containing tcl configuration (tclConfig.sh),
below is an example:

    $ export CC=g++
    $ cd tcl-leveldb
    $ ./configure --with-tcl=/opt/activetcl/lib
    $ make
    $ make install


Implement commands
=====

The key and data is interpreted by Tcl as a byte array.

### Basic usage
leveldb version

The ommand `leveldb version` return a list of the form {major minor} 
for the major and minor levels of the LevelDB release.

### Database
leveldb open -path path ?-create_if_missing BOOLEAN? ?-error_if_exists BOOLEAN? 
 ?-paranoid_checks BOOLEAN? ?-write_buffer_size size? ?-max_open_files number? 
 ?-compression type?   
leveldb repair name  
leveldb destroy name  
DB_HANDLE get key ?-fillCache BOOLEAN?  
DB_HANDLE put key value ?-sync BOOLEAN?  
DB_HANDLE delete key ?-sync BOOLEAN?  
DB_HANDLE write BAT_HANDLE  
DB_HANDLE batch  
DB_HANDLE iterator  
DB_HANDLE getApproximateSizes start limit  
DB_HANDLE getProperty property  
DB_HANDLE close  
IT_HANDLE seektofirst  
IT_HANDLE seektolast  
IT_HANDLE seek key  
IT_HANDLE valid  
IT_HANDLE next  
IT_HANDLE prev  
IT_HANDLE key  
IT_HANDLE value  
IT_HANDLE close  
BAT_HANDLE put key value  
BAT_HANDLE delete key  
BAT_HANDLE close  

The command `leveldb open` create a database handle. -path option is the path 
of the database to open. -compression type supports "no" and "snappy".
(Please link to snappy, I think it is the default type.)

If a DB cannot be opened, you may attempt to call `leveldb repair` this method
to resurrect as much of the contents of the database as possible. Some data
may be lost, so be careful when calling this function on a database that
contains important information.

`leveldb destroy` destroy the contents of the specified database.
Be very careful using this method.

`DB_HANDLE iterator` create an Iterator handle.

`DB_HANDLE batch` create a WriteBatch handle. Users can use `DB_HANDLE write`
to apply a set of updates.

`DB_HANDLE getApproximateSizes` can used to get the approximate number of
bytes.

`DB_HANDLE getProperty` can get DB export properties about their state via
this method.  If it is a valid property, returns its current value.


Examples
=====

A basic example:

    package require leveldb

    set dbi [leveldb open -path "./testdb" -create_if_missing 1]
    $dbi put "test1" "1234567890"
    set value [$dbi get "test1"]
    puts $value

    $dbi delete "test1"
    $dbi put "test2" "2345678901"
    $dbi put "test3" "3456789010"
    $dbi put "test4" "4567890102"
    $dbi put "test5" "5678901023"

    set it [$dbi iterator]
    for {$it seektofirst} {[$it valid] == 1} {$it next} {
        set key [$it key]
        set value [$it value]
        puts "Iterator --- I get $key: $value"
    }
    $it close
    $dbi close

WriteBatch example:

    package require leveldb

    set dbi [leveldb open -path "./testdb" -create_if_missing 1]
    set bat [$dbi batch]
    $bat put "test1" "1234567890"
    $bat put "test2" "2345678901"
    $bat put "test3" "3456789010"
    $bat put "test4" "4567890102"
    $bat put "test5" "5678901023"
    $dbi write $bat
    $bat close

    set it [$dbi iterator]
    for {$it seektofirst} {[$it valid] == 1} {$it next} {
        set key [$it key]
        set value [$it value]
        puts "Iterator --- I get $key: $value"
    }
    $it close
    $dbi close


