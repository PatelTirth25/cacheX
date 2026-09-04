use std::time::Duration;

use cachex_server::storage::{Aof, AofEntry, Store};

fn test_store(capacity: usize) -> Store {
    Store::new(capacity, "test-node".to_string(), "127.0.0.1:1".to_string())
}

#[test]
fn set_and_get() {
    let mut store = test_store(100);
    store.set("key".to_string(), b"value".to_vec(), None);
    assert_eq!(store.get("key"), Some(b"value".to_vec()));
}

#[test]
fn get_missing_key_returns_none() {
    let mut store = test_store(100);
    assert_eq!(store.get("missing"), None);
}

#[test]
fn get_after_eventual_expiry() {
    let mut store = test_store(100);
    store.set(
        "key".to_string(),
        b"value".to_vec(),
        Some(Duration::from_millis(1)),
    );
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(store.get("key"), None);
}

#[test]
fn set_overwrites_value_without_growing_store() {
    let mut store = test_store(100);
    store.set("key".to_string(), b"v1".to_vec(), None);
    store.set("key".to_string(), b"v2".to_vec(), None);
    assert_eq!(store.get("key"), Some(b"v2".to_vec()));
    assert_eq!(store.keys(), 1);
}

#[test]
fn delete_removes_key_then_reports_missing() {
    let mut store = test_store(100);
    store.set("key".to_string(), b"value".to_vec(), None);
    assert!(store.delete("key"));
    assert_eq!(store.get("key"), None);
    assert!(!store.delete("key"));
}

#[test]
fn lru_evicts_least_recently_used() {
    let mut store = test_store(2);
    store.set("a".to_string(), b"1".to_vec(), None);
    store.set("b".to_string(), b"2".to_vec(), None);
    store.get("a");
    store.set("c".to_string(), b"3".to_vec(), None);

    assert_eq!(store.get("a"), Some(b"1".to_vec()));
    assert_eq!(store.get("b"), None);
    assert_eq!(store.get("c"), Some(b"3".to_vec()));
    assert_eq!(store.keys(), 2);
}

#[test]
fn updating_existing_key_does_not_evict() {
    let mut store = test_store(2);
    store.set("a".to_string(), b"1".to_vec(), None);
    store.set("b".to_string(), b"2".to_vec(), None);
    store.set("a".to_string(), b"updated".to_vec(), None);

    assert_eq!(store.keys(), 2);
    assert_eq!(store.get("a"), Some(b"updated".to_vec()));
    assert_eq!(store.get("b"), Some(b"2".to_vec()));
}

#[test]
fn memory_bytes_scales_with_entries() {
    let mut store = test_store(100);
    store.set("a".to_string(), b"x".to_vec(), None);
    store.set("b".to_string(), b"y".to_vec(), None);
    assert!(store.memory_bytes() > 0);
}

#[test]
fn aof_recovery_roundtrip() {
    let path = std::env::temp_dir().join(format!("cachex_test_{}.aof", std::process::id()));
    let _ = std::fs::remove_file(&path);

    {
        let mut store = test_store(100);
        store.set_aof(Aof::open(path.to_str().unwrap()));
        store.set("k1".to_string(), b"v1".to_vec(), None);
        store.set(
            "k2".to_string(),
            b"v2".to_vec(),
            Some(Duration::from_secs(3600)),
        );
        store.delete("k1");
    }

    let mut recovered = Store::recover(
        path.to_str().unwrap(),
        100,
        "test-node".to_string(),
        "127.0.0.1:1".to_string(),
    );

    assert_eq!(recovered.get("k1"), None);
    assert_eq!(recovered.get("k2"), Some(b"v2".to_vec()));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn aof_recovery_skips_expired_ttl() {
    let path = std::env::temp_dir().join(format!("cachex_test_expired_{}.aof", std::process::id()));
    let _ = std::fs::remove_file(&path);

    {
        let mut store = test_store(100);
        store.set_aof(Aof::open(path.to_str().unwrap()));
        let mut a = Aof::open(path.to_str().unwrap());
        a.append(&AofEntry::Set {
            key: "old".to_string(),
            value: b"x".to_vec(),
            expire_at: Some(1), // already long past
        });
        a.append(&AofEntry::Set {
            key: "fresh".to_string(),
            value: b"y".to_vec(),
            expire_at: None,
        });
    }

    let mut recovered = Store::recover(
        path.to_str().unwrap(),
        100,
        "test-node".to_string(),
        "127.0.0.1:1".to_string(),
    );

    assert_eq!(recovered.get("old"), None);
    assert_eq!(recovered.get("fresh"), Some(b"y".to_vec()));

    let _ = std::fs::remove_file(&path);
}
