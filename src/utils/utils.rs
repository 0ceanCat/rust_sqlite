use std::path::{Path, PathBuf};
use std::{fs, ptr};
use std::ffi::OsString;

#[macro_export]
macro_rules! to_u8_array {
    ($s:ident, $size: expr) => {
        {
           let mut array: [u8; $size] = [0; $size];
            let bytes = $s.as_bytes();
            array[..bytes.len()].copy_from_slice(bytes);
            array
        }
    };
}

#[macro_export]
macro_rules! build_path {
    ( $( $x:expr ),* ) => {
        {
           let mut path = PathBuf::new();
           $(
                path.push($x);
            )*
            path
        }
    };
}

pub trait ToU8 {
    fn to_u8(&self) -> u8;
}

impl ToU8 for bool {
    fn to_u8(&self) -> u8 {
        if *self { 1 } else { 0 }
    }
}

pub(crate) fn copy_nonoverlapping<T>(src: *const T, dst: *mut T, count: usize) {
    unsafe {
        ptr::copy_nonoverlapping(src, dst, count);
    }
}

pub(crate) fn copy<T>(src: *const T, dst: *mut T, count: usize) {
    unsafe {
        ptr::copy(src, dst, count);
    }
}

pub(crate) fn indent(level: usize) {
  for _ in 0..level {
      print!(" ")
  }
}


pub(crate) fn is_folder_empty(folder_path: &Path) -> bool {
    match fs::read_dir(folder_path) {
        Ok(entries) => {
            for entry in entries {
                if let Ok(_) = entry {
                    return false;
                }
            }
            true
        }
        Err(_) => {
            true
        }
    }
}

pub(crate) fn is_file_exists(folder_path: &Path) -> bool {
    match fs::metadata(folder_path) {
        Ok(_) => true,
        Err(_) => false,
    }
}

pub(crate) fn u8_array_to_string(array: &[u8]) -> String {
    let end = array.iter().position(|c| *c == 0).unwrap_or(array.len());
    String::from_utf8_lossy(&array[..end]).to_string()
}

pub(crate) fn list_files_of_folder(folder_path: &PathBuf) -> Result<Vec<(OsString, PathBuf)>, String> {
    match fs::read_dir(folder_path) {
        Ok(entries) => {
            let mut files: Vec<(OsString, PathBuf)> = vec![];
            for entry in entries {
                if let Ok(dirEntry) = entry {
                   files.push((dirEntry.file_name(), dirEntry.path()));
                }
            }

            Ok(files)
        }
        Err(_) => {
            return Err(format!("Can not open directory {}", folder_path.to_str().unwrap()))
        }
    }
}