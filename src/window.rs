use std::sync::atomic::{AtomicI64, Ordering};

pub struct InsertWindow {
    tolerance: i64,
    max_timestamp: AtomicI64,
}

impl InsertWindow {
    pub fn new(tolerance: i64) -> Self {
        Self {
            tolerance,
            max_timestamp: AtomicI64::default(),
        }
    }

    pub fn contains(&self, timestamp: i64) -> bool {
        (self.max_timestamp.load(Ordering::SeqCst) - timestamp) <= self.tolerance
    }

    pub fn update(&self, timestamp: i64) {
        self.max_timestamp
            .fetch_update(Ordering::SeqCst, Ordering::Relaxed, |x| {
                Some(i64::max(x, timestamp))
            })
            .unwrap();
    }
}

#[cfg(test)]
pub mod tests {
    use super::InsertWindow;

    #[test]
    fn test_insert_window_no_tolerance() {
        let window = InsertWindow::new(0);
        assert!(window.contains(0));

        window.update(1000);
        assert!(!window.contains(999));
        assert!(window.contains(1000));
        assert!(window.contains(2000));
    }

    #[test]
    fn test_insert_window_tolerance() {
        let window = InsertWindow::new(20);
        assert!(window.contains(0));

        window.update(1000);
        assert!(!window.contains(979));
        assert!(window.contains(999));
        assert!(window.contains(1000));
        assert!(window.contains(2000));
    }
}
