use std::path::Path;

use crate::Repository;

impl Repository {
    pub fn save(&self, base: &Path) {
        let bytes = postcard::to_allocvec(self).expect("failed to serialize repository");
        std::fs::write(base.join(crate::REPOSITORY_FILE), bytes)
            .expect("failed to write repository file");
    }

    pub fn load(base: &Path) -> Self {
        let bytes = std::fs::read(base.join(crate::REPOSITORY_FILE))
            .expect("failed to read repository file");
        postcard::from_bytes(&bytes).expect("failed to deserialize repository")
    }
}
