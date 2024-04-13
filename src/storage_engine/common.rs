use std::ptr;

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