use std::sync::atomic::{AtomicI64, Ordering};

#[derive(Default)]
pub struct Bounds {
    // Tolerance for inserting out-of-order data points.
    // Given the last data point inserted with timestamp t,
    // the insert window will allow a new data point with a timestamp
    // between [t-past, t+future).
    // An past value of 0 means out-of-order inserts are not
    // allowed.
    // Leaving past as None will default the insert window to whatever
    // the storage allows to be written.
    pub past: Option<u64>,
    pub future: Option<u64>,
}

pub struct InsertWindow {
    bounds: Bounds,
    max_timestamp: AtomicI64,
}

impl InsertWindow {
    pub fn new(bounds: Bounds) -> Self {
        Self {
            bounds,
            max_timestamp: AtomicI64::default(),
        }
    }

    fn future_contains(&self, timestamp: i64) -> bool {
        match self.bounds.future {
            Some(f) => {
                (timestamp - self.max_timestamp.load(Ordering::SeqCst)) <= f.try_into().unwrap()
            }
            None => true,
        }
    }

    fn past_contains(&self, timestamp: i64) -> bool {
        match self.bounds.past {
            Some(p) => {
                (self.max_timestamp.load(Ordering::SeqCst) - timestamp) <= p.try_into().unwrap()
            }
            None => true,
        }
    }

    pub fn contains(&self, timestamp: i64) -> bool {
        self.future_contains(timestamp) && self.past_contains(timestamp)
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
    use crate::window::Bounds;

    use super::InsertWindow;

    #[test]
    fn test_insert_window_strict_past() {
        let window = InsertWindow::new(Bounds {
            past: Some(0),
            future: None,
        });
        assert!(window.contains(0));

        window.update(1000);
        assert!(!window.contains(999));
        assert!(window.contains(1000));
        assert!(window.contains(2000));
    }

    #[test]
    fn test_insert_window_relaxed_past() {
        let window = InsertWindow::new(Bounds {
            past: Some(20),
            future: None,
        });
        assert!(window.contains(0));

        window.update(1000);
        assert!(!window.contains(979));
        assert!(window.contains(999));
        assert!(window.contains(1000));
        assert!(window.contains(2000));
    }

    #[test]
    fn test_insert_window_relaxed_future() {
        let window = InsertWindow::new(Bounds {
            past: Some(20),
            future: Some(20),
        });
        assert!(window.contains(0));

        window.update(1000);
        assert!(!window.contains(1021));
        assert!(window.contains(1020));
        assert!(!window.contains(2000));
    }
}
