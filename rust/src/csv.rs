use std::cell::RefCell;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use memchr::memchr_iter;

const CHUNK_BYTES: u64 = 4 * 1024 * 1024;

pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(target_arch = "x86_64")]
    if (32..=128).contains(&line.len())
        && std::is_x86_feature_detected!("avx2")
        && std::is_x86_feature_detected!("bmi1")
        && std::is_x86_feature_detected!("popcnt")
    {
        // SAFETY: the CPU supports AVX2 and each input contains 1 to 4 vectors.
        return unsafe { fields_avx2(line) };
    }
    fields_portable(line)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,popcnt")]
unsafe fn fields_avx2<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use std::arch::x86_64::*;

    let mut comma_masks = [0_u32; 4];
    let mut quote_count = 0;
    for (index, mask) in comma_masks
        .iter_mut()
        .take(line.len().div_ceil(32))
        .enumerate()
    {
        let start = index * 32;
        let load_at = start.min(line.len() - 32);
        let skip = start - load_at;
        // SAFETY: dispatch checked AVX2 and load_at + 32 <= line.len().
        // The shift discards the bytes from an overlapping final load.
        unsafe {
            let bytes = _mm256_loadu_si256(line.as_ptr().add(load_at).cast());
            let commas = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b',' as i8));
            let quotes = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8));
            *mask = (_mm256_movemask_epi8(commas) as u32) >> skip;
            quote_count += ((_mm256_movemask_epi8(quotes) as u32) >> skip).count_ones();
        }
    }

    let mut low = u64::from(comma_masks[0]) | (u64::from(comma_masks[1]) << 32);
    let mut high = u64::from(comma_masks[2]) | (u64::from(comma_masks[3]) << 32);
    if (low.count_ones() + high.count_ones()) as usize + 1 != N {
        return None;
    }
    let mut result = [&[][..]; N];
    let mut matched_quotes = 0;
    let mut start = 0;
    for field in &mut result {
        let end = if low != 0 {
            let end = low.trailing_zeros() as usize;
            low &= low - 1;
            end
        } else if high != 0 {
            let end = 64 + high.trailing_zeros() as usize;
            high &= high - 1;
            end
        } else {
            line.len()
        };
        let left = line[start.min(line.len() - 1)];
        let right = line[end.saturating_sub(1)];
        let trim = usize::from((end - start >= 2) & (left == b'"') & (right == b'"'));
        matched_quotes += 2 * trim as u32;
        *field = &line[start + trim..end - trim];
        start = end + 1;
    }
    // Every quote must be one of the two outer bytes of its own field.
    (matched_quotes == quote_count).then_some(result)
}

fn fields_portable<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut commas = memchr_iter(b',', line);
    let mut quotes = memchr_iter(b'"', line);
    let mut next_quote = quotes.next();
    let mut start = 0;
    let mut fields = [&[][..]; N];
    for (index, field) in fields.iter_mut().enumerate() {
        let end = match commas.next() {
            Some(at) if index + 1 < N => at,
            None if index + 1 == N => line.len(),
            _ => return None,
        };
        *field = if next_quote.is_some_and(|position| position < end) {
            if next_quote != Some(start) || quotes.next() != Some(end - 1) {
                return None;
            }
            next_quote = quotes.next();
            &line[start + 1..end - 1]
        } else {
            &line[start..end]
        };
        start = end + 1;
    }
    Some(fields)
}

#[cfg(test)]
mod field_tests {
    use super::*;

    #[test]
    fn vector_boundaries_agree_with_portable_csv() {
        // Move every delimiter and quote across the 32/64/96-byte boundaries,
        // including malformed rows, empty fields, CRs and a final empty field.
        for padding in 0..140 {
            for suffix in [
                &b",b,\"c\",,d,\"\",\r"[..],
                &b",\"b\",c,d,e,f,"[..],
                &b",b,c,d,e,\"f\"\"\",g"[..],
                &b",b,c,d,e,f,g\""[..],
                &b",b,c,d,e,f"[..],
                &b",b,c,d,e,f,g,h"[..],
            ] {
                let mut line = vec![b'a'; padding];
                line.extend_from_slice(suffix);
                assert_eq!(fields::<7>(&line), fields_portable::<7>(&line));
            }
        }
    }
}

pub fn parts(path: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "csv") && path.is_file() {
            paths.push(path);
        }
    }
    Ok(paths)
}

pub struct Chunk {
    path: Arc<Path>,
    start: u64,
    end: u64,
}

struct ScanBuffer {
    bytes: Vec<u8>,
    partial_line: Vec<u8>,
}

thread_local! {
    static SCAN_BUFFER: RefCell<ScanBuffer> = RefCell::new(ScanBuffer {
        bytes: vec![0; 128 * 1024],
        partial_line: Vec::new(),
    });
}

impl Chunk {
    pub fn scan(&self, mut row: impl FnMut(&[u8])) -> Result<()> {
        let mut file = File::open(&self.path)?;
        let mut offset = self.start.saturating_sub(1);
        file.seek(SeekFrom::Start(offset))?;
        SCAN_BUFFER.with_borrow_mut(|buffer| {
            let ScanBuffer {
                bytes,
                partial_line,
            } = buffer;
            partial_line.clear();
            // Skip the header, or the preceding chunk's last record. Looking back
            // one byte makes a record that starts exactly on the boundary ours.
            let mut skip_first = true;
            loop {
                let len = match file.read(bytes) {
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    result => result?,
                };
                if len == 0 {
                    if !partial_line.is_empty() {
                        row(partial_line);
                    }
                    return Ok(());
                }
                let block = &bytes[..len];
                let mut start = 0;
                for end in memchr_iter(b'\n', block) {
                    if skip_first {
                        skip_first = false;
                    } else if partial_line.is_empty() {
                        row(&block[start..end]);
                    } else {
                        partial_line.extend_from_slice(&block[start..end]);
                        row(partial_line);
                        partial_line.clear();
                    }
                    start = end + 1;
                    if offset + start as u64 >= self.end {
                        return Ok(());
                    }
                }
                offset += len as u64;
                if !skip_first {
                    partial_line.extend_from_slice(&block[start..]);
                } else if offset >= self.end {
                    return Ok(());
                }
            }
        })
    }
}

impl Chunk {
    pub fn scan_parsed<const N: usize>(&self, mut row: impl FnMut([&[u8]; N])) -> Result<()> {
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx2")
            && std::is_x86_feature_detected!("bmi1")
            && std::is_x86_feature_detected!("popcnt")
        {
            // SAFETY: CPU features are checked once for this entire chunk.
            return unsafe { self.scan_parsed_avx2(row) };
        }
        self.scan(|line| {
            if let Some(values) = fields_portable(line) {
                row(values);
            }
        })
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2,bmi1,popcnt")]
    unsafe fn scan_parsed_avx2<const N: usize>(
        &self,
        mut row: impl FnMut([&[u8]; N]),
    ) -> Result<()> {
        self.scan(|line| {
            let values = if (32..=128).contains(&line.len()) {
                // SAFETY: target features are active and the vector loads fit.
                unsafe { fields_avx2(line) }
            } else {
                fields_portable(line)
            };
            if let Some(values) = values {
                row(values);
            }
        })
    }
}

pub fn chunks(directory: &Path) -> Result<Vec<Chunk>> {
    let mut paths = parts(directory)?
        .into_iter()
        .map(|path| {
            let length = path.metadata()?.len();
            Ok((length, Arc::<Path>::from(path)))
        })
        .collect::<Result<Vec<_>>>()?;
    paths.sort_unstable_by_key(|(len, _)| std::cmp::Reverse(*len));
    let jobs = paths.into_iter().flat_map(|(len, path)| {
        (0..len)
            .step_by(CHUNK_BYTES as usize)
            .map(move |start| Chunk {
                path: path.clone(),
                start,
                end: (start + CHUNK_BYTES).min(len),
            })
    });
    Ok(jobs.collect())
}

#[cfg(test)]
mod scan_tests {
    use super::*;

    #[test]
    fn chunks_preserve_lines_across_read_buffers_and_job_boundaries() -> Result<()> {
        let path =
            std::env::temp_dir().join(format!("shopping-csv-boundaries-{}", std::process::id()));
        let block = 128 * 1024;
        let mut data = b"header\n".to_vec();
        let expected = [
            vec![b'a'; block - data.len() - 1],
            vec![b'b'; 3 * block + 17],
            Vec::new(),
            b"last\r".to_vec(),
        ];
        for (position, record) in expected.iter().enumerate() {
            data.extend_from_slice(record);
            if position + 1 < expected.len() {
                data.push(b'\n');
            }
        }
        fs::write(&path, &data)?;
        let mut actual = Vec::new();
        for start in (0..data.len()).step_by(block) {
            Chunk {
                path: Arc::from(path.as_path()),
                start: start as u64,
                end: (start + block).min(data.len()) as u64,
            }
            .scan(|line| actual.push(line.to_vec()))?;
        }
        fs::remove_file(&path)?;
        assert_eq!(actual.as_slice(), expected);
        Ok(())
    }
}

pub fn scan_file(path: &Path, row: impl FnMut(&[u8])) -> Result<()> {
    Chunk {
        path: Arc::from(path),
        start: 0,
        end: u64::MAX,
    }
    .scan(row)
    .with_context(|| format!("reading {}", path.display()))
}
