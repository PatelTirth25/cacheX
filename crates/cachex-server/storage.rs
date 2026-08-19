use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type SharedStore = Arc>;

pub struct Store {
    data: HashMap>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option> {
        self.data.get(key).cloned()
    }

    pub fn set(&mut self, key: String, value: Vec) {
        self.data.insert(key, value);
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }
    
    pub fn len(&self) -> usize {
        self.data.len()
    }
}
