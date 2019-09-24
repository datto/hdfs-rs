/* This file is part of libhdfs-sys.
 *
 * Copyright © 2020 Datto, Inc.
 * Author: Alex Parrill <aparrill@datto.com>
 *
 * Licensed under the Mozilla Public License Version 2.0
 * Fedora-License-Identifier: MPLv2.0
 * SPDX-2.0-License-Identifier: MPL-2.0
 * SPDX-3.0-License-Identifier: MPL-2.0
 *
 * libhdfs-sys is free software.
 * For more information on the license, see LICENSE.
 * For more information on free software, see <https://www.gnu.org/philosophy/free-sw.en.html>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at <https://mozilla.org/MPL/2.0/>.
 */



use bindgen;
use java_locator;
use std::env;
use std::path::PathBuf;

fn main() {
	println!("cargo:rerun-if-env-changed=RSHDFS_HEADER_DIR");
	println!("cargo:rerun-if-env-changed=RSHDFS_LIB_DIR");
	println!("cargo:rerun-if-env-changed=RSHDFS_STATIC");
	
	let libjvm_path = java_locator::locate_jvm_dyn_library()
		.unwrap();
	println!("cargo:rustc-link-search=native={}", libjvm_path);
	
	let header_path = if let Some(dir) = env::var_os("RSHDFS_HEADER_DIR") {
		let mut path = PathBuf::from(dir);
		path.push("hdfs.h");
		path
	} else {
		PathBuf::from("hdfs.h")
	};
	let header_path = header_path.into_os_string().into_string().expect("Could not convert RSHDFS_HEADER_DIR to a string");
	
	if let Ok(dir) = env::var("RSHDFS_LIB_DIR") {
		println!("cargo:rustc-link-search=native={}", dir);
	}
	
	let kind = if env::var("RSHDFS_STATIC").unwrap_or("".into()) != "" {
		println!("cargo:rustc-link-lib=dylib=jvm");
		"static"
	} else {
		"dylib"
	};
	println!("cargo:rustc-link-lib={}=hdfs", kind);
	
	let bindings = bindgen::Builder::default()
		.header(header_path)
		.opaque_type("hdfs_internal")
		.generate()
		.expect("Could not generate bindings");
	
	let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
	bindings
		.write_to_file(out_path.join("bindings.rs"))
		.expect("Could not write bindings");
}
