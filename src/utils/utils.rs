use std::{fs, ptr};
use std::ffi::OsString;
use std::path::PathBuf;

#[macro_export]
macro_rules! to_u8_array {
    ($s:ident, $size: expr) => {{
        let mut array: [u8; $size] = [0; $size];
        let bytes = $s.as_bytes();
        array[..bytes.len()].copy_from_slice(bytes);
        array
    }};
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
        if *self {
            1
        } else {
            0
        }
    }
}

pub trait ToInt {
    fn to_int(&self) -> i32;
}

impl ToInt for char {
    fn to_int(&self) -> i32 {
        *self as i32 - 0x30
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

pub(crate) fn u8_array_to_string(array: &[u8]) -> String {
    let end = array.iter().position(|c| *c == 0).unwrap_or(array.len());
    String::from_utf8_lossy(&array[..end]).to_string()
}

pub(crate) fn list_files_of_folder(
    folder_path: &PathBuf,
) -> Result<Vec<(OsString, PathBuf)>, String> {
    match fs::read_dir(folder_path) {
        Ok(entries) => {
            let mut files: Vec<(OsString, PathBuf)> = vec![];
            for entry in entries {
                if let Ok(dir_entry) = entry {
                    files.push((dir_entry.file_name(), dir_entry.path()));
                }
            }

            Ok(files)
        }
        Err(_) => {
            return Err(format!(
                "Can not open directory {}",
                folder_path.to_str().unwrap()
            ))
        }
    }
}
