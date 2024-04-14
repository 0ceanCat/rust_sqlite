use std::path::Path;
use std::{fs, ptr};

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

pub(crate) fn u8_array_to_string(array: &[u8]) -> String {
    let end = array.iter().position(|c| *c == 0).unwrap_or(array.len());
    String::from_utf8_lossy(&array[..end]).to_string()
}