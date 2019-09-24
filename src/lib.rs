/* This file is part of hdfs-rs.
 *
 * Copyright © 2020 Datto, Inc.
 * Author: Alex Parrill <aparrill@datto.com>
 *
 * Licensed under the Mozilla Public License Version 2.0
 * Fedora-License-Identifier: MPLv2.0
 * SPDX-2.0-License-Identifier: MPL-2.0
 * SPDX-3.0-License-Identifier: MPL-2.0
 *
 * hdfs-rs is free software.
 * For more information on the license, see LICENSE.
 * For more information on free software, see <https://www.gnu.org/philosophy/free-sw.en.html>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at <https://mozilla.org/MPL/2.0/>.
 */


//! High-level, safe bindings for `libhdfs`, for reading and writing to HDFS.
//! 
//! ```ignore
//! let mut builder = hdfs::HdfsBuilder::new();
//! builder.name_node(None); // Local FS
//! let connection = builder.connect()?;
//! let files = connection.list_dir("/");
//! println!("{:?}", files);
//! ```
//! 
//! Most functions return `io::Result`, since `libhdfs` returns errors through `errno`.
//! 
//! Building and Running
//! --------------------
//! 
//! Using `libhdfs` is a bit tricky, because it requires JNI and is usually not installed at the
//! system level. To help, you can specify some environmental variables while building:
//! 
//! * `RSHDFS_HEADER_DIR`: Directory with `hdfs.h` in it
//! * `RSHDFS_LIB_DIR`: Directory with `libhdfs.so` or `libhdfs.a` in it
//! * `RSHDFS_STATIC`: If set to a non-empty string, link `libhdfs.a` instead of `libhdfs.so` (the default).
//!   You will probably need `RUSTFLAGS="-C relocation-model=dynamic-no-pic"` to make this work.
//! * `JAVA_HOME`: For linking to `libjni` when using a static library. If not set, the build script will
//!   try to guess based on where the `java` executable in your path is symlinked to.
//! 
//! When running an executable using this library, you need to ensure two things for `libhdfs`:
//! 
//! * `libjni.so` is loadable. You may need to set `LD_LIBRARY_PATH` to the directory that it's in.
//!   You may also want to add the Hadoop native libraries, from `hadoop jnipath`, to allow short circuit
//!   reads and other optimizations.
//! * `CLASSPATH` is set up to load all of the hadoop libraries, without wildcards. You can do this with
//!   `export CLASSPATH="$(hadoop classpath --glob)"`
//! 
//! Signals
//! -------
//! 
//! Note that the JVM that libhdfs uses internally registers its own signal handlers, potentially overwriting
//! the ones your application sets up. For Linux, the JVM registers handlers for SIGSEGV, SIGBUS, SIGFPE, SIGPIPE,
//! SIGILL, SIGQUIT, SIGTERM, SIGINT, SIGHUP, SIGUSR1, and SIGUSR2. You can set the `LIBHDFS_OPTS` environmental
//! variable (ex. with `std::env::set_var`) to `-Xrs` to tell libhdfs' JVM to not hook onto SIGQUIT, SIGTERM, SIGINT,
//! and SIGHUP, or register your signal handler after at least one `HdfsConnection` has been created. See
//! [Oracle's documentation on signals](https://www.oracle.com/technetwork/java/javase/signals-139944.html)
//! for more info.

pub extern crate libhdfs_sys;

use std::convert::TryFrom;
use std::ffi::{CStr, CString};
use std::io;
use std::mem;
use std::os::raw::*;
use std::ptr::{self, NonNull};
use std::time::{Duration, SystemTime};

/// Allocate a new `CString` from a `str` slice. Panics if it contains null bytes.
fn str_to_cstr(s: &str) -> CString {
	CString::new(s.as_bytes()).expect("string contains null byte")
}
/// Allocates a new `String` from a C string pointer.
unsafe fn cstr_to_str(p: *const c_char) -> String {
	CStr::from_ptr(p).to_string_lossy().into_owned()
}
/// Allocate a new `CString` from a `str` slice, puts it in a vec, and returns the C
/// pointer. Useful if the `CString` needs to stay around for awhile.
fn str_to_cstr_pooled<'a>(pool: &'a mut Vec<CString>, s: &str) -> *const c_char {
	let s = str_to_cstr(s);
	let index = pool.len();
	pool.push(s);
	return pool[index].as_ptr();
}

/// Checks for a zero return code. If it's zero, returns `Ok(())`, otherwisee
/// returns the last OS error.
fn check_rt(rt: c_int) -> io::Result<()> {
	if rt == 0 {
		return Ok(());
	} else {
		return Err(io::Error::last_os_error());
	}
}

/// Gets a pointer from an `Option<CStr>`; either the pointer to the string or `NULL`.
fn opt_cstr_as_ptr<T: AsRef<CStr>>(s: &Option<T>) -> *const c_char {
	s.as_ref().map(|v| v.as_ref().as_ptr()).unwrap_or(ptr::null())
}

/// Converts `time_t` to a `SystemTime` object.
fn time_t_to_systime(v: &libhdfs_sys::tTime) -> SystemTime {
	SystemTime::UNIX_EPOCH + Duration::from_secs(*v as u64)
}



/// Builds an HDFS connection
pub struct HdfsBuilder {
	// Only `None` when `connect` consumes it
	p: Option<NonNull<libhdfs_sys::hdfsBuilder>>,
	// Builder doesn't copy strings, it copies pointers, so need
	// to keep the strings alive.
	allocated_strings: Vec<CString>
}
impl HdfsBuilder {
	fn ptr(&self) -> *mut libhdfs_sys::hdfsBuilder {
		self.p.as_ref().unwrap().as_ptr()
	}
	
	/// Creates a new builder
	/// 
	/// Panics
	/// ======
	/// Panics if the builder object could not be allocated.
	pub fn new() -> Self {
		let p = unsafe {
			NonNull::new(libhdfs_sys::hdfsNewBuilder())
				.expect("Could not create hdfs builder")
		};
		Self {p: Some(p), allocated_strings: vec![]}
	}
	
	/// Sets a Hadoop configuration property.
	pub fn conf_set(&mut self, key: &str, value: &str) -> io::Result<()> {
		let key_p = str_to_cstr_pooled(&mut self.allocated_strings, key);
		let value_p = str_to_cstr_pooled(&mut self.allocated_strings, value);
		
		let rt = unsafe { libhdfs_sys::hdfsBuilderConfSetStr(self.ptr(), key_p, value_p) };
		return check_rt(rt);
	}
	
	/// Forces creation of a new instance, rather than re-using a cached one.
	pub fn force_new_instance(&mut self) {
		unsafe { libhdfs_sys::hdfsBuilderSetForceNewInstance(self.ptr()); }
	}
	
	/// Specifies the name node to connect to.
	/// 
	/// If `Some("default")`, connects to the default server specified in `hdfs-site.xml.
	/// If `None`, connects to the local filesystem.
	/// Otherwise, should specify a host and optional port of a namenode to connect to.
	pub fn name_node(&mut self, host: Option<&str>) {
		let host_p = host.map(|host| str_to_cstr_pooled(&mut self.allocated_strings, host)).unwrap_or(ptr::null());
		unsafe { libhdfs_sys::hdfsBuilderSetNameNode(self.ptr(), host_p); }
	}
	
	/// Specifies the username to connect as
	pub fn user_name(&mut self, name: &str) {
		let name_p = str_to_cstr_pooled(&mut self.allocated_strings, name);
		unsafe { libhdfs_sys::hdfsBuilderSetUserName(self.ptr(), name_p); }
	}
	
	/// Connects to HDFS, consuming the builder.
	pub fn connect(mut self) -> io::Result<HdfsConnection> {
		let p_maybe = unsafe {
			NonNull::new(libhdfs_sys::hdfsBuilderConnect(self.ptr()))
		};
		self.p = None;
		mem::drop(self);

		if let Some(p) = p_maybe {
			return Ok(HdfsConnection {p});
		} else {
			return Err(io::Error::last_os_error());
		}
	}
}
impl Drop for HdfsBuilder {
	fn drop(&mut self) {
		if let Some(p) = self.p.take() {
			unsafe {
				libhdfs_sys::hdfsFreeBuilder(p.as_ptr());
			}
		}
	}
}
unsafe impl Send for HdfsBuilder {}


/// Connection to an HDFS filesystem.
pub struct HdfsConnection {
	p: NonNull<libhdfs_sys::hdfs_internal>,
}
impl HdfsConnection {
	/// Creates a builder for creating a connection.
	/// 
	/// Same as `HdfsBuilder::new()`.
	pub fn builder() -> HdfsBuilder {
		HdfsBuilder::new()
	}
	
	/// Checks if a path exists in the filesystem.
	pub fn exists(&self, path: &str) -> io::Result<bool> {
		let path = str_to_cstr(path);
		
		// This API is stupid
		let rt = unsafe { libhdfs_sys::hdfsExists(self.p.as_ptr(), path.as_ptr()) };
		if rt == 0 {
			return Ok(true);
		}
		let err = io::Error::last_os_error();
		if err.kind() == io::ErrorKind::NotFound {
			return Ok(false);
		}
		return Err(err);
	}
	
	/// Changes the permission bits of a file
	pub fn chmod(&self, path: &str, mode: u16) -> io::Result<()> {
		let path = str_to_cstr(path);
		let rt = unsafe { libhdfs_sys::hdfsChmod(self.p.as_ptr(), path.as_ptr(), mode as c_short) };
		return check_rt(rt);
	}
	
	/// Changes the owner and group of a file.
	/// 
	/// Specifying `None` for either the owner or group means that it won't be updated.
	pub fn chown(&self, path: &str, owner: Option<&str>, group: Option<&str>) -> io::Result<()> {
		let path = str_to_cstr(path);
		let owner = owner.map(|s| str_to_cstr(s));
		let group = group.map(|s| str_to_cstr(s));
		let rt = unsafe { libhdfs_sys::hdfsChown(self.p.as_ptr(), path.as_ptr(), opt_cstr_as_ptr(&owner), opt_cstr_as_ptr(&group)) };
		return check_rt(rt);
	}
	
	/// Deletes a file.
	/// 
	/// Will not delete non-empty directories unless `recursive` is true
	pub fn delete(&self, path: &str, recursive: bool) -> io::Result<()> {
		let path = str_to_cstr(path);
		let rt = unsafe { libhdfs_sys::hdfsDelete(self.p.as_ptr(), path.as_ptr(), if recursive { 1 } else { 0 }) };
		return check_rt(rt);
	}
	
	/// Truncates a file to a certain size
	pub fn truncate(&self, path: &str, size: libhdfs_sys::tOffset) -> io::Result<()> {
		let path = str_to_cstr(path);
		let rt = unsafe { libhdfs_sys::hdfsTruncateFile(self.p.as_ptr(), path.as_ptr(), size) };
		return check_rt(rt);
	}
	
	/// Renames a file
	pub fn rename(&self, src: &str, dest: &str) -> io::Result<()> {
		let src = str_to_cstr(src);
		let dest = str_to_cstr(dest);
		let rt = unsafe { libhdfs_sys::hdfsRename(self.p.as_ptr(), src.as_ptr(), dest.as_ptr()) };
		return check_rt(rt);
	}
	
	/// Moves a file to a different HDFS filesystem
	pub fn move_to(&self, src: &str, dest_fs: &HdfsConnection, dest: &str) -> io::Result<()> {
		let src = str_to_cstr(src);
		let dest = str_to_cstr(dest);
		let rt = unsafe { libhdfs_sys::hdfsMove(
			self.p.as_ptr(),
			src.as_ptr(),
			dest_fs.p.as_ptr(),
			dest.as_ptr()
		)};
		return check_rt(rt);
	}
	
	/// Lists the contents of a directory
	pub fn list_dir(&self, path: &str) -> io::Result<Vec<HdfsDirectoryEntry>> {
		let path = str_to_cstr(&path);
		let mut num_entries = 123i32; // Initialize to non-zero for empty dir detection
		let p_maybe = unsafe {
			NonNull::new(libhdfs_sys::hdfsListDirectory(self.p.as_ptr(), path.as_ptr(), &mut num_entries as *mut _))
		};
		
		let p = match p_maybe {
			Some(p) => p,
			None if num_entries == 0 => {
				// Empty directory
				return Ok(vec![]);
			},
			None => {
				return Err(io::Error::last_os_error());
			},
		};
		
		let mut v = Vec::<HdfsDirectoryEntry>::with_capacity(num_entries as usize);
		for i in 0..num_entries {
			let converted = unsafe { HdfsDirectoryEntry::from_raw(&*(p.as_ptr().add(i as usize))) };
			v.push(converted);
		}
		unsafe { libhdfs_sys::hdfsFreeFileInfo(p.as_ptr(), num_entries); }
		Ok(v)
	}
	
	fn stream_builder(&self, path: &str, flags: u32) -> io::Result<HdfsStreamBuilder> {
		let path = str_to_cstr(path);
		let p_maybe = unsafe {
			NonNull::new(libhdfs_sys::hdfsStreamBuilderAlloc(self.p.as_ptr(), path.as_ptr(), flags as i32))
		};
		if let Some(p) = p_maybe {
			return Ok(HdfsStreamBuilder { fs: self, p });
		} else {
			return Err(io::Error::last_os_error());
		}
	}
	
	/// Creates a stream builder for opening a file for reading
	pub fn open_read_builder(&self, path: &str) -> io::Result<HdfsStreamBuilder> {
		self.stream_builder(path, libhdfs_sys::O_RDONLY)
	}
	
	/// Creates a stream builder for opening a file for writing, creating if it does not exist
	pub fn open_create_builder(&self, path: &str) -> io::Result<HdfsStreamBuilder> {
		self.stream_builder(path, libhdfs_sys::O_WRONLY)
	}
	
	/// Creates a stream builder for opening a file for appending, creating if it does not exist
	pub fn open_append_builder(&self, path: &str) -> io::Result<HdfsStreamBuilder> {
		self.stream_builder(path, libhdfs_sys::O_WRONLY | libhdfs_sys::O_APPEND)
	}
	
	/// Opens a file for reading, using the default stream builder arguments
	pub fn open_read(&self, path: &str) -> io::Result<HdfsFile> {
		self.open_read_builder(path)?.build()
	}
	
	/// Opens a file for writing, creating if it does not exist, using the default stream builder arguments
	pub fn open_create(&self, path: &str) -> io::Result<HdfsFile> {
		self.open_create_builder(path)?.build()
	}
	
	/// Opens a file for appending, creating if it does not exist, using the default stream builder arguments
	pub fn open_append(&self, path: &str) -> io::Result<HdfsFile> {
		self.open_append_builder(path)?.build()
	}
}
impl Drop for HdfsConnection {
	fn drop(&mut self) {
		unsafe {
			libhdfs_sys::hdfsDisconnect(self.p.as_ptr());
		}
	}
}
unsafe impl Send for HdfsConnection {}

/// Builder for opening files, allowing advanced options to be set
pub struct HdfsStreamBuilder<'a> {
	fs: &'a HdfsConnection,
	p: NonNull<libhdfs_sys::hdfsStreamBuilder>,
}
impl<'a> HdfsStreamBuilder<'a> {
	/// Sets the client-side buffer size.
	pub fn buffer_size(&mut self, size: i32) -> io::Result<()> {
		let rt = unsafe { libhdfs_sys::hdfsStreamBuilderSetBufferSize(self.p.as_ptr(), size) };
		return check_rt(rt);
	}
	/// Sets the default block size for writing new files.
	/// 
	/// Will return an error for read streams, since this option isn't relevant for them.
	pub fn default_block_size(&mut self, size: i64) -> io::Result<()> {
		let rt = unsafe { libhdfs_sys::hdfsStreamBuilderSetDefaultBlockSize(self.p.as_ptr(), size) };
		return check_rt(rt);
	}
	/// Sets the replication factor for writing new files.
	/// 
	/// Will return an error for read streams, since this option isn't relevant for them.
	pub fn replication(&mut self, repl: i16) -> io::Result<()> {
		let rt = unsafe { libhdfs_sys::hdfsStreamBuilderSetReplication(self.p.as_ptr(), repl) };
		return check_rt(rt);
	}
	
	/// Builds the stream, opening the file.
	pub fn build(self) -> io::Result<HdfsFile<'a>> {
		let fs = self.fs;
		let p_maybe = unsafe {
			NonNull::new(libhdfs_sys::hdfsStreamBuilderBuild(self.p.as_ptr()))
		};
		mem::forget(self);
		if let Some(p) = p_maybe {
			return Ok(HdfsFile { fs, p });
		} else {
			return Err(io::Error::last_os_error());
		}
	}
}
impl<'a> Drop for HdfsStreamBuilder<'a> {
	fn drop(&mut self) {
		unsafe {
			libhdfs_sys::hdfsStreamBuilderFree(self.p.as_ptr());
		}
	}
}



/// Open HDFS file.
/// 
/// Supports the `Read`, `Write`, and `Seek` interfaces.
/// 
/// The lifetime ensures that you must close all files before the HDFS connection.
pub struct HdfsFile<'a> {
	fs: &'a HdfsConnection,
	p: NonNull<libhdfs_sys::hdfsFile_internal>,
}
impl<'a> HdfsFile<'a> {
	/// Requests that the file be flushed to disk, blocking until it does so.
	/// 
	/// `flush` sends the client buffer to HDFS only. This function waits until the data
	/// is safely on disk.
	pub fn sync(&mut self) -> io::Result<()> {
		let rt = unsafe { libhdfs_sys::hdfsHSync(self.fs.p.as_ptr(), self.p.as_ptr()) };
		return check_rt(rt);
	}
}
impl<'a> io::Read for HdfsFile<'a> {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let num_to_read = buf.len().min(libhdfs_sys::tSize::max_value() as usize);
		let rt = unsafe { libhdfs_sys::hdfsRead(
			self.fs.p.as_ptr(),
			self.p.as_ptr(),
			buf.as_mut_ptr() as *mut c_void,
			num_to_read as libhdfs_sys::tSize
		)};
		if rt < 0 {
			return Err(io::Error::last_os_error());
		}
		return Ok(rt as usize);
	}
}
impl<'a> io::Write for HdfsFile<'a> {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		let num_to_read = buf.len().min(libhdfs_sys::tSize::max_value() as usize);
		let rt = unsafe { libhdfs_sys::hdfsWrite(
			self.fs.p.as_ptr(),
			self.p.as_ptr(),
			buf.as_ptr() as *const c_void,
			num_to_read as libhdfs_sys::tSize
		)};
		if rt < 0 {
			return Err(io::Error::last_os_error());
		}
		return Ok(rt as usize);
	}
	
	fn flush(&mut self) -> io::Result<()> {
		let rt = unsafe { libhdfs_sys::hdfsFlush(self.fs.p.as_ptr(), self.p.as_ptr()) };
		return check_rt(rt);
	}
}
impl<'a> io::Seek for HdfsFile<'a> {
	/// Note: only `io::SeekFrom::Current(n)` and `io::SeekFrom::Start(n)` is supported, due to API limitations.
	/// `Current(n)` does a tell.
	fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
		let offset = match pos {
			io::SeekFrom::Start(offset) => {
				let offset = libhdfs_sys::tOffset::try_from(offset)
					.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "seek offset overflow"))?;
				offset
			},
			io::SeekFrom::Current(delta) => {
				let current_pos = unsafe { libhdfs_sys::hdfsTell(self.fs.p.as_ptr(), self.p.as_ptr()) };
				if current_pos < 0 {
					return Err(io::Error::last_os_error());
				}
				if delta == 0 {
					return Ok(current_pos as u64);
				}
				
				let delta = libhdfs_sys::tOffset::try_from(delta)
					.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "seek offset overflow"))?;
				let new_pos = current_pos.checked_add(delta)
					.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek offset overflow"))?;
				new_pos
			},
			_ => { return Err(io::Error::new(io::ErrorKind::Other, "seek on HdfsFile only supports SeekFrom::Start and SeekFrom::Current")); }
		};
		
		let rt = unsafe { libhdfs_sys::hdfsSeek(self.fs.p.as_ptr(), self.p.as_ptr(), offset) };
		return check_rt(rt).map(|_| offset as u64);
	}
}
impl<'a> Drop for HdfsFile<'a> {
	fn drop(&mut self) {
		unsafe {
			libhdfs_sys::hdfsCloseFile(self.fs.p.as_ptr(), self.p.as_ptr());
		}
	}
}

/// Entry returned by `HdfsConnection::list_dir`.
#[derive(Debug,Clone)]
pub struct HdfsDirectoryEntry {
	/// What type of entry? File or Directory?
	pub kind: HdfsDirectoryEntryKind,
	/// Name of the file, as an absolute url (ex. `hdfs://host/a/b/c`)
	pub name: String,
	/// The time the file was last modified
	pub last_modified: SystemTime,
	/// The size of the file
	pub size: u64,
	/// The replication factor of the file
	pub replication: u16,
	/// The block size of the file
	pub block_size: u64,
	/// The owner of the file
	pub owner: String,
	/// The group of the file
	pub group: String,
	/// Permission bits on the file
	pub permissions: u16,
	/// The time the file was last accessed.
	pub last_access: SystemTime,
}
impl HdfsDirectoryEntry {
	unsafe fn from_raw(raw: &libhdfs_sys::hdfsFileInfo) -> Self {
		Self {
			kind: HdfsDirectoryEntryKind::from(raw.mKind),
			name: cstr_to_str(raw.mName),
			last_modified: time_t_to_systime(&raw.mLastMod),
			size: raw.mSize as u64,
			replication: raw.mReplication as u16,
			block_size: raw.mBlockSize as u64,
			owner: cstr_to_str(raw.mOwner),
			group: cstr_to_str(raw.mGroup),
			permissions: raw.mPermissions as u16,
			last_access: time_t_to_systime(&raw.mLastAccess),
		}
	}
}

/// What type of file an HDFS entry can be.
#[derive(Debug,Clone,Copy)]
#[repr(u8)]
pub enum HdfsDirectoryEntryKind {
	File,
	Directory,
	Unrecognized(libhdfs_sys::tObjectKind),
}
impl From<libhdfs_sys::tObjectKind> for HdfsDirectoryEntryKind {
	fn from(value: libhdfs_sys::tObjectKind) -> Self {
		match value {
			libhdfs_sys::tObjectKind_kObjectKindDirectory => HdfsDirectoryEntryKind::Directory,
			libhdfs_sys::tObjectKind_kObjectKindFile => HdfsDirectoryEntryKind::File,
			_ => HdfsDirectoryEntryKind::Unrecognized(value),
		}
	}
}
