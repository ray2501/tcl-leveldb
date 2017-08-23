%{!?directory:%define directory /usr}

%define buildroot %{_tmppath}/%{name}-%{version}

Name:          tcl-leveldb
Summary:       Tcl interface for LevelDB
Version:       0.1.1
Release:       1
License:       MIT
Group:         Development/Libraries/Tcl
Source:        %name-%version.tar.gz
URL:           https://github.com/ray2501/tcl-leveldb
BuildRequires: autoconf
BuildRequires: make
BuildRequires: leveldb-devel
BuildRequires: tcl-devel >= 8.5
Requires:      tcl >= 8.5
BuildRoot:     %{buildroot}

%description
LevelDB is a fast key-value storage library written at Google that provides
an ordered mapping from string keys to string values.

This extension provides an easy to use interface for accessing LevelDB
database files from Tcl.

%prep
%setup -q -n %{name}-%{version}

%build
export CC=g++
./configure \
	--prefix=%{directory} \
	--exec-prefix=%{directory} \
	--libdir=%{directory}/%{_lib}
make 

%install
make DESTDIR=%{buildroot} pkglibdir=%{directory}/%{_lib}/tcl/%{name}%{version} install

%clean
rm -rf %buildroot

%files
%defattr(-,root,root)
%{directory}/%{_lib}/tcl
