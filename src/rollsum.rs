// Ported from: https://github.com/bup/bup/blob/4f0b883db6245abd8f67d30bf8852559f18b6366/lib/bup/bupsplit.h

const WINDOWBITS: u32 = 6;
const WINDOWSIZE: u32 = 1 << WINDOWBITS;
const ROLLSUM_CHAR_OFFSET: u32 = 31;

pub struct Rollsum {
    s1: u32,
    s2: u32,
    wofs: usize,
    window: [u8; WINDOWSIZE as usize]
}

impl Rollsum {
    pub fn new() -> Self {
        Self {
            s1: WINDOWSIZE * ROLLSUM_CHAR_OFFSET,
            s2: WINDOWSIZE * (WINDOWSIZE-1) * ROLLSUM_CHAR_OFFSET,
            wofs: 0,
            window: [0; WINDOWSIZE as usize]
        }
    }

    fn add(&mut self, drop: u8, add: u8) {
        self.s1 = self.s1.wrapping_add(add as u32).wrapping_sub(drop as u32);
        self.s2 = self.s2
            .wrapping_add(self.s1)
            .wrapping_sub(WINDOWSIZE * (drop as u32 + ROLLSUM_CHAR_OFFSET));
    }

    pub fn roll(&mut self, ch: u8) {
        self.add(self.window[self.wofs], ch);
        self.window[self.wofs] = ch;
        self.wofs = (self.wofs + 1) % WINDOWSIZE as usize;
    }

    pub fn sum(&mut self, buffer: &[u8]) {
        for i in 0..buffer.len() {
            self.roll(buffer[i]);
        }
    }

    pub fn digest(&self) -> u32 {
        return (self.s1 << 16) | (self.s2 & 0xffff);
    }
}