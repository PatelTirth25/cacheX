use std::ptr;

struct Node {
    key: String,
    prev: *mut Node,
    next: *mut Node,
}

pub struct LruList {
    head: *mut Node,
    tail: *mut Node,
    map: std::collections::HashMap<String, *mut Node>,
}

unsafe impl Send for LruList {}

impl LruList {
    pub fn new() -> Self {
        let head = Box::into_raw(Box::new(Node {
            key: String::new(),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }));
        let tail = Box::into_raw(Box::new(Node {
            key: String::new(),
            prev: head,
            next: ptr::null_mut(),
        }));
        unsafe {
            (*head).next = tail;
        }

        Self {
            head,
            tail,
            map: std::collections::HashMap::new(),
        }
    }

    pub fn push_front(&mut self, key: String) {
        let node = Box::into_raw(Box::new(Node {
            key: key.clone(),
            prev: self.head,
            next: unsafe { (*self.head).next },
        }));
        unsafe {
            (*(*self.head).next).prev = node;
            (*self.head).next = node;
        }
        self.map.insert(key, node);
    }

    pub fn access(&mut self, key: &str) {
        if let Some(&node) = self.map.get(key) {
            self.unlink(node);
            unsafe {
                (*node).prev = self.head;
                (*node).next = (*self.head).next;
                (*(*self.head).next).prev = node;
                (*self.head).next = node;
            }
        }
    }

    pub fn remove(&mut self, key: &str) {
        if let Some(node) = self.map.remove(key) {
            self.unlink(node);
            unsafe {
                drop(Box::from_raw(node));
            }
        }
    }

    pub fn pop_back(&mut self) -> Option<String> {
        let node = unsafe { (*self.tail).prev };
        if node == self.head {
            return None;
        }
        self.unlink(node);
        let key = unsafe { (*node).key.clone() };
        unsafe {
            drop(Box::from_raw(node));
        }
        self.map.remove(&key);
        Some(key)
    }

    #[allow(dead_code)]
    pub fn iter_keys(&self) -> impl Iterator<Item = &String> {
        let mut keys = Vec::new();
        let mut current = unsafe { (*self.head).next };
        while current != self.tail {
            unsafe {
                keys.push(&(*current).key);
                current = (*current).next;
            }
        }
        keys.into_iter()
    }

    fn unlink(&self, node: *mut Node) {
        unsafe {
            (*(*node).prev).next = (*node).next;
            (*(*node).next).prev = (*node).prev;
        }
    }
}

impl Drop for LruList {
    fn drop(&mut self) {
        let mut current = unsafe { (*self.head).next };
        while current != self.tail {
            let next = unsafe { (*current).next };
            unsafe {
                drop(Box::from_raw(current));
            }
            current = next;
        }
        unsafe {
            drop(Box::from_raw(self.head));
            drop(Box::from_raw(self.tail));
        }
    }
}
