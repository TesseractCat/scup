use std::path::Path;

use crate::Repository;

impl Repository {
    pub fn save(&self, base: &Path) {
        let bytes = postcard::to_allocvec(self).expect("failed to serialize repository");
        std::fs::write(base.join(".syncup/repository"), bytes)
            .expect("failed to write .syncup/repository");
    }

    pub fn load(base: &Path) -> Self {
        let bytes = std::fs::read(base.join(".syncup/repository"))
            .expect("failed to read .syncup/repository");
        postcard::from_bytes(&bytes).expect("failed to deserialize repository")
    }
}
