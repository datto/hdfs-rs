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



use hdfs::*;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use structopt::StructOpt;
use structopt::clap::AppSettings;

#[derive(Debug,StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Subcommand {
	/// Lists a directory
	Ls {
		/// Path to list
		dir: String,
	},
	/// Downloads a file
	Get {
		path: String,
		dest: Option<PathBuf>,
	},
	/// Uploads a file
	#[structopt(setting=AppSettings::AllowMissingPositional)]
	Put {
		src: Option<PathBuf>,
		dest: String,
	},
	/// Renames a file
	Mv {
		src: String,
		dest: String,
	},
	/// Removes a file
	Rm {
		path: String,
		#[structopt(short="r")]
		recursive: bool,
	},
}

#[derive(Debug,StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct Args {
	/// Nameserver URL to connect to
	#[structopt(short="N")]
	pub name_server: Option<String>,
	/// Username to connect as
	#[structopt(short="U")]
	pub username: Option<String>,
	
	#[structopt(subcommand)]
	pub subcommand: Subcommand,
}
impl Args {
	pub fn connect(&self) -> io::Result<HdfsConnection> {
		let mut builder = HdfsConnection::builder();
		builder.name_node(self.name_server.as_ref().map(|s| s.as_str()));
		if let Some(name) = self.username.as_ref() {
			builder.user_name(name);
		}
		builder.connect()
	}
}

fn main() {
	if let Err(err) = real_main() {
		eprintln!("{}", err);
		::std::process::exit(1);
	}
}

fn real_main() -> Result<(), String> {
	let app = Args::clap()
	//	.global_setting(AppSettings::AllowMissingPositional)
	;
	let args = Args::from_clap(&app.get_matches());
	
	let fs = args.connect()
		.map_err(|e| format!("Could not connect to hdfs: {}", e))?;
	
	match args.subcommand {
		Subcommand::Ls { dir } => {
			let entries = fs.list_dir(&dir)
				.map_err(|e| format!("Could not list directory: {}", e))?;
			
			for entry in entries.into_iter() {
				println!("{:<80} {:>10} {:>10} {:>10}",
					entry.name,
					entry.size,
					entry.owner,
					entry.group,
				);
			}
		},
		Subcommand::Get { path, dest } => {
			let mut in_file = fs.open_read(&path)
				.map_err(|e| format!("Could not open input file: {}", e))?;
			
			let stdout = io::stdout();
			
			let mut out_file: Box<dyn Write> = match dest {
				Some(out_path) => {
					Box::new(File::create(&out_path)
						.map_err(|e| format!("Could not create output file: {}", e))?
					)
				},
				None => {
					Box::new(stdout.lock())
				},
			};
			
			io::copy(&mut in_file, &mut out_file)
				.map_err(|e| format!("Could not copy data: {}", e))?;
			out_file.flush()
				.map_err(|e| format!("Could not copy data: {}", e))?;
		},
		Subcommand::Put { src, dest } => {
			let mut out_file = fs.open_create(&dest)
				.map_err(|e| format!("Could not open output file: {}", e))?;
			
			let stdin = io::stdin();
			
			let mut in_file: Box<dyn Read> = match src {
				Some(out_path) => {
					Box::new(File::open(&out_path)
						.map_err(|e| format!("Could not open input file: {}", e))?
					)
				},
				None => {
					Box::new(stdin.lock())
				},
			};
			
			io::copy(&mut in_file, &mut out_file)
				.map_err(|e| format!("Could not copy data: {}", e))?;
			out_file.flush()
				.map_err(|e| format!("Could not copy data: {}", e))?;
		},
		Subcommand::Mv { src, dest } => {
			fs.rename(&src, &dest)
				.map_err(|e| format!("Could not rename: {}", e))?;
		},
		Subcommand::Rm { path, recursive } => {
			fs.delete(&path, recursive)
				.map_err(|e| format!("Could not delete: {}", e))?;
		},
	}
	
	Ok(())
}
