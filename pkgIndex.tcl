# -*- tcl -*-
# Tcl package index file, version 1.1
#
if {[package vsatisfies [package provide Tcl] 9.0-]} {
    package ifneeded leveldb 0.3.0 \
	    [list load [file join $dir libtcl9leveldb0.3.0.so] [string totitle leveldb]]
} else {
    package ifneeded leveldb 0.3.0 \
	    [list load [file join $dir libleveldb0.3.0.so] [string totitle leveldb]]
}
