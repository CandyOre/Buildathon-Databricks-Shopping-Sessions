use std::fs::{self, File};
use std::path::Path;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::aggregate::{CampaignDays, SessionBatch};
use crate::input::{Dimensions, PersonNames};

const BATCH_SIZE: usize = 65_536;
const USER_PARTS: usize = 8;

struct Options {
    rows: usize,
    buffer: usize,
    plain: bool,
    merge: bool,
    workers: usize,
}

fn options() -> &'static Options {
    static OPTIONS: OnceLock<Options> = OnceLock::new();
    OPTIONS.get_or_init(|| Options {
        rows: crate::runtime::number("PERF_GROUP_ROWS", BATCH_SIZE).clamp(1, i32::MAX as usize),
        buffer: crate::runtime::number("PERF_WRITE_BUFFER", 256 << 10).clamp(4096, 16 << 20),
        plain: crate::runtime::flag("PERF_USER_PLAIN"),
        merge: crate::runtime::flag("PERF_MERGE_GROUPS"),
        workers: crate::runtime::number("PERF_WRITERS", USER_PARTS).clamp(1, USER_PARTS),
    })
}

#[derive(Default)]
pub struct PreparedNames {
    // (exclusive end row, start byte of the length-prefixed UTF-8 name).
    runs: Vec<(usize, usize)>,
    bytes: Vec<u8>,
}

impl PreparedNames {
    pub(crate) fn push_run(&mut self, end: usize, name: &str) -> Result<()> {
        let offset = self.bytes.len();
        direct::plain_string(&mut self.bytes, name)?;
        self.runs.push((end, offset));
        Ok(())
    }

    fn append(&mut self, part: Self) {
        let row_base = self.runs.last().map_or(0, |r| r.0);
        let byte_base = self.bytes.len();
        self.runs.extend(
            part.runs
                .into_iter()
                .map(|(end, at)| (row_base + end, byte_base + at)),
        );
        self.bytes.extend(part.bytes);
    }
}

pub(crate) fn aggregate_names(names: &crate::input::Names) -> Option<&crate::input::Names> {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    (*ENABLED.get_or_init(|| crate::runtime::flag("PERF_AGGREGATE_NAMES"))).then_some(names)
}

pub fn prepare_names(
    batches: &[SessionBatch],
    names: &PersonNames,
) -> Result<Option<Vec<PreparedNames>>> {
    if !crate::runtime::flag("PERF_PREPARE_NAMES") || crate::runtime::flag("PERF_AGGREGATE_NAMES") {
        return Ok(None);
    }
    (0..USER_PARTS)
        .into_par_iter()
        .map(|part| {
            let start = (part * batches.len()).div_ceil(USER_PARTS);
            let end = ((part + 1) * batches.len()).div_ceil(USER_PARTS);
            let mut dictionary = PreparedNames::default();
            let mut buffer = [0; 9];
            let mut last = None;
            for (i, &person) in batches[start..end]
                .iter()
                .flat_map(|b| &b.daily.person)
                .enumerate()
            {
                if last != Some(person) {
                    let name = names.get(person, &mut buffer);
                    dictionary.push_run(i + 1, name)?;
                    last = Some(person);
                } else {
                    dictionary.runs.last_mut().unwrap().0 = i + 1;
                }
            }
            Ok(dictionary)
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

pub fn write(
    output: &Path,
    users: Vec<SessionBatch>,
    campaigns: CampaignDays,
    names: PersonNames,
    dimensions: Dimensions,
    dictionaries: Option<Vec<PreparedNames>>,
) -> Result<()> {
    let shards = users.len();
    let mut parts: [Vec<_>; USER_PARTS] = std::array::from_fn(|_| Vec::new());
    for (i, batch) in users.into_iter().enumerate() {
        parts[i * USER_PARTS / shards].push(batch);
    }
    let output = output.to_owned();
    let names = Arc::new(names);
    let dimensions = Arc::new(dimensions);
    let mut dictionaries = dictionaries.unwrap_or_default().into_iter();
    let mut lanes: Vec<Vec<_>> = (0..options().workers).map(|_| Vec::new()).collect();
    for (part, batches) in parts.into_iter().enumerate() {
        lanes[part % options().workers].push((part, batches, dictionaries.next()));
    }
    let mut tasks: Vec<crate::runtime::Work<Result<()>>> = Vec::new();
    let mut campaign = Some((campaigns, dimensions.clone()));
    for (index, lane) in lanes.into_iter().enumerate() {
        let (dir, ids, dims) = (output.clone(), names.clone(), dimensions.clone());
        let first = if index == 0 && crate::runtime::flag("PERF_CAMPAIGN_FIRST") {
            campaign.take()
        } else {
            None
        };
        tasks.push(Box::new(move || {
            if let Some((rows, dims)) = first {
                write_campaigns(&dir, rows, &dims)?;
            }
            for (part, batches, dictionary) in lane {
                direct::users(&dir, part, batches, &ids, &dims, dictionary.as_ref())?;
            }
            Ok(())
        }));
    }
    if let Some((rows, dims)) = campaign {
        tasks.push(Box::new(move || write_campaigns(&output, rows, &dims)));
    }
    crate::runtime::run(tasks).into_iter().collect()
}

fn open(
    output: &Path,
    table: &str,
    part: usize,
    columns: &[(&str, DataType, bool)],
) -> Result<(SchemaRef, ArrowWriter<File>)> {
    let directory = output.join(table);
    fs::create_dir_all(&directory)?;
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, kind, nullable)| Field::new(*name, kind.clone(), *nullable))
            .collect::<Vec<_>>(),
    ));
    let properties = WriterProperties::builder()
        .set_compression(if crate::runtime::flag("PERF_CAMPAIGN_PLAIN") {
            Compression::UNCOMPRESSED
        } else {
            Compression::SNAPPY
        })
        .set_max_row_group_size(BATCH_SIZE)
        .build();
    let writer = ArrowWriter::try_new(
        File::create(directory.join(format!("part-{part}.parquet")))?,
        schema.clone(),
        Some(properties),
    )?;
    Ok((schema, writer))
}

fn write_campaigns(output: &Path, campaigns: CampaignDays, dimensions: &Dimensions) -> Result<()> {
    let clock = crate::csv::work_enabled().then(std::time::Instant::now);
    let (schema, mut writer) = open(
        output,
        "campaign_daily",
        0,
        &[
            ("campaign_id", DataType::Utf8, false),
            ("channel", DataType::Utf8, false),
            ("day", DataType::Date32, false),
            ("sessions", DataType::Int64, false),
            ("purchasing_sessions", DataType::Int64, false),
            ("revenue_cents", DataType::Int64, false),
            ("assisted_sessions", DataType::Int64, false),
        ],
    )?;
    let mut iter = campaigns.into_iter();
    let mut rows = Vec::with_capacity(BATCH_SIZE.min(iter.size_hint().0));
    loop {
        rows.clear();
        rows.extend(iter.by_ref().take(BATCH_SIZE));
        if rows.is_empty() {
            break;
        }
        let lookup = |id: u32| &dimensions.campaigns[id as usize];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|((id, _), _)| lookup(*id).0.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|((id, _), _)| lookup(*id).1.as_str()),
            )),
            Arc::new(Date32Array::from_iter_values(
                rows.iter().map(|((_, day), _)| *day),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, row)| row.sessions),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, row)| row.purchasing_sessions),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, row)| row.revenue),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, row)| row.assisted_sessions),
            )),
        ];
        writer.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
    }
    let meta = writer.close()?;
    if let Some(clock) = clock {
        eprintln!(
            "profile {}",
            serde_json::json!({"stage":"campaign_writer_work",
            "elapsed_s":clock.elapsed().as_secs_f64(),"row_groups":meta.row_groups.len(),"rows":meta.num_rows})
        );
    }
    Ok(())
}

mod direct {
    use std::borrow::Cow;
    use std::io::{BufWriter, Write};

    use super::*;
    use crate::aggregate::DailyColumns;
    use parquet::format as f;
    use parquet::thrift::{TCompactOutputProtocol, TSerializable};
    use snap::raw::{Encoder, max_compress_len};

    const TARGET_STRING_PAGE: usize = 8 * 1024 * 1024;
    const COLUMNS: [(&str, f::Type); 6] = [
        ("user_id", f::Type::BYTE_ARRAY),
        ("day", f::Type::INT32),
        ("sessions", f::Type::INT64),
        ("events", f::Type::INT64),
        ("revenue_cents", f::Type::INT64),
        ("top_category", f::Type::BYTE_ARRAY),
    ];

    fn schema() -> Vec<f::SchemaElement> {
        let root = f::SchemaElement::new(
            None,
            None,
            None,
            "schema".into(),
            Some(6),
            None,
            None,
            None,
            None,
            None,
        );
        let mut schema = vec![root];
        for (i, &(name, ty)) in COLUMNS.iter().enumerate() {
            let (converted, logical) = match i {
                0 | 5 => (
                    Some(f::ConvertedType::UTF8),
                    Some(f::LogicalType::STRING(f::StringType::new())),
                ),
                1 => (
                    Some(f::ConvertedType::DATE),
                    Some(f::LogicalType::DATE(f::DateType::new())),
                ),
                _ => (None, None),
            };
            let repetition = if i == 5 {
                f::FieldRepetitionType::OPTIONAL
            } else {
                f::FieldRepetitionType::REQUIRED
            };
            schema.push(f::SchemaElement::new(
                Some(ty),
                None,
                Some(repetition),
                name.into(),
                None,
                converted,
                None,
                None,
                None,
                logical,
            ));
        }
        schema
    }

    fn varint(mut value: usize, out: &mut Vec<u8>) {
        while value >= 128 {
            out.push(value as u8 | 128);
            value >>= 7;
        }
        out.push(value as u8);
    }

    pub(super) fn plain_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
        out.extend_from_slice(&i32::try_from(value.len())?.to_le_bytes());
        out.extend_from_slice(value.as_bytes());
        Ok(())
    }

    macro_rules! integers {
        ($name:ident, $ty:ty) => {
            fn $name(values: &[$ty]) -> Cow<'_, [u8]> {
                #[cfg(target_endian = "little")]
                {
                    // SAFETY: fixed-width primitive integers are initialized
                    // and have no padding. Their native little-endian bytes
                    // equal the PLAIN representation on these hosts.
                    Cow::Borrowed(unsafe {
                        std::slice::from_raw_parts(
                            values.as_ptr().cast(),
                            std::mem::size_of_val(values),
                        )
                    })
                }
                #[cfg(target_endian = "big")]
                {
                    Cow::Owned(values.iter().flat_map(|v| v.to_le_bytes()).collect())
                }
            }
        };
    }
    integers!(i32_bytes, i32);
    integers!(i64_bytes, i64);

    struct CategoryDictionary {
        bytes: Vec<u8>,
        entries: usize,
        width: usize,
    }

    impl CategoryDictionary {
        fn build(values: &[String]) -> Result<Option<Self>> {
            if values.is_empty() || values.len() > 65536 {
                return Ok(None);
            }
            let mut bytes = Vec::new();
            for value in values {
                if bytes.len() + 4 + value.len() > TARGET_STRING_PAGE {
                    return Ok(None);
                }
                plain_string(&mut bytes, value)?;
            }
            Ok(Some(Self {
                bytes,
                entries: values.len(),
                width: if values.len() <= 256 { 1 } else { 2 },
            }))
        }
    }

    fn packed_header(out: &mut Vec<u8>, count: usize, bytes_per_index: usize) -> usize {
        out.push((8 * bytes_per_index) as u8);
        let groups = count.div_ceil(8);
        if groups != 0 {
            varint((groups << 1) | 1, out);
        }
        (groups * 8 - count) * bytes_per_index
    }

    #[derive(Default)]
    struct Numeric {
        dictionary: Vec<u8>,
        values: Vec<u8>,
        extra_ids: ahash::AHashMap<i64, u8>,
        extras: Vec<i64>,
    }

    impl Numeric {
        fn day(&mut self, days: &[i32]) -> bool {
            if days.len() < 128 {
                return false;
            }
            let mut min = i32::MAX;
            let mut max = i32::MIN;
            for &day in days {
                min = min.min(day);
                max = max.max(day);
            }
            if i64::from(max) - i64::from(min) >= 256 {
                return false;
            }
            self.dictionary.clear();
            for day in min..=max {
                self.dictionary.extend_from_slice(&day.to_le_bytes());
            }
            self.values.clear();
            let pad = packed_header(&mut self.values, days.len(), 1);
            self.values
                .extend(days.iter().map(|&v| v.wrapping_sub(min) as u8));
            self.values.resize(self.values.len() + pad, 0);
            true
        }

        fn count(&mut self, values: &[i64]) -> bool {
            if values.len() < 128 {
                return false;
            }
            const DIRECT: usize = 64;
            let mut small_entries = 1;
            self.extras.clear();
            self.extra_ids.clear();
            self.values.clear();
            let pad = packed_header(&mut self.values, values.len(), 1);
            for &value in values {
                let id = if (value as u64) < DIRECT as u64 {
                    small_entries = small_entries.max(value as usize + 1);
                    value as u8
                } else if let Some(&id) = self.extra_ids.get(&value) {
                    id
                } else {
                    let id = DIRECT + self.extras.len();
                    if id == 256 {
                        return false;
                    }
                    self.extras.push(value);
                    self.extra_ids.insert(value, id as u8);
                    id as u8
                };
                self.values.push(id);
            }
            self.values.resize(self.values.len() + pad, 0);
            self.dictionary.clear();
            let prefix = if self.extras.is_empty() {
                small_entries
            } else {
                DIRECT
            };
            for value in 0..prefix {
                self.dictionary
                    .extend_from_slice(&(value as i64).to_le_bytes());
            }
            for &value in &self.extras {
                self.dictionary.extend_from_slice(&value.to_le_bytes());
            }
            true
        }
    }

    #[derive(Default)]
    struct Group {
        // One dictionary value and RLE run per contiguous canonical person.
        dictionary: Vec<u8>,
        runs: Vec<usize>,
        indices: Vec<u8>,
        categories: Vec<u8>,
    }

    impl Group {
        #[cfg(test)]
        fn prepare(
            &mut self,
            daily: &DailyColumns,
            start: usize,
            names: &PersonNames,
            dimensions: &Dimensions,
            categories: Option<&CategoryDictionary>,
        ) -> Result<usize> {
            self.prepare_rows(daily, start, names, dimensions, categories, None, 0)
        }

        fn prepare_rows(
            &mut self,
            daily: &DailyColumns,
            start: usize,
            names: &PersonNames,
            dimensions: &Dimensions,
            categories: Option<&CategoryDictionary>,
            prepared: Option<&PreparedNames>,
            base: usize,
        ) -> Result<usize> {
            self.dictionary.clear();
            self.runs.clear();
            let mut previous = None;
            let mut cat_bytes = 0;
            let mut present = 0;
            let mut end = start;
            let mut buffer = [0; 9];
            let limit = daily.person.len().min(start.saturating_add(options().rows));
            let mut run = prepared.map_or(0, |p| p.runs.partition_point(|r| r.0 <= base + start));
            while end != limit {
                let (changed, name, encoded) = if let Some(p) = prepared {
                    let advance = p.runs[run].0 == base + end;
                    run += usize::from(advance);
                    let changed = end == start || advance;
                    let encoded = changed.then(|| {
                        let stop = p.runs.get(run + 1).map_or(p.bytes.len(), |r| r.1);
                        &p.bytes[p.runs[run].1..stop]
                    });
                    (changed, None, encoded)
                } else {
                    let person = daily.person[end];
                    let changed = previous != Some(person);
                    previous = Some(person);
                    let name = if changed {
                        Some(names.get(person, &mut buffer))
                    } else {
                        None
                    };
                    (changed, name, None)
                };
                let extra_name =
                    encoded.map_or_else(|| name.map_or(0, |s| 4 + s.len()), |s| s.len());
                let extra_category = daily.category[end].map_or(0, |id| {
                    categories
                        .map_or_else(|| 4 + dimensions.categories[id as usize].len(), |d| d.width)
                });
                // A single long string may require its own row group. The
                // checked page length below enforces the format's i32 limit.
                if end > start
                    && (self.dictionary.len() + extra_name > TARGET_STRING_PAGE
                        || cat_bytes + extra_category > TARGET_STRING_PAGE)
                {
                    break;
                }
                if changed {
                    if let Some(bytes) = encoded {
                        self.dictionary.extend_from_slice(bytes);
                    } else {
                        plain_string(&mut self.dictionary, name.unwrap())?;
                    }
                    self.runs.push(1);
                } else {
                    *self.runs.last_mut().unwrap() += 1;
                }
                present += usize::from(daily.category[end].is_some());
                cat_bytes += extra_category;
                end += 1;
            }

            let width = (usize::BITS - (self.runs.len() - 1).leading_zeros()).max(1) as usize;
            let nbytes = width.div_ceil(8);
            self.indices.clear();
            self.indices.push(width as u8);
            for (id, &count) in self.runs.iter().enumerate() {
                varint(count << 1, &mut self.indices);
                self.indices.extend_from_slice(&id.to_le_bytes()[..nbytes]);
            }

            // V1 definition levels have a 4-byte length prefix. A single
            // bit-packed run contains one bit per row, padded to groups of 8.
            let values = &daily.category[start..end];
            let groups = values.len().div_ceil(8);
            self.categories.clear();
            self.categories.extend_from_slice(&[0; 4]);
            varint((groups << 1) | 1, &mut self.categories);
            let bitmap = self.categories.len();
            self.categories.resize(bitmap + groups, 0);
            let encoded_len = u32::try_from(self.categories.len() - 4)?;
            self.categories[..4].copy_from_slice(&encoded_len.to_le_bytes());
            self.categories.reserve(cat_bytes + 24);
            let pad = categories.map_or(0, |dict| {
                packed_header(&mut self.categories, present, dict.width)
            });
            for (i, value) in values.iter().enumerate() {
                if let Some(id) = value {
                    self.categories[bitmap + i / 8] |= 1 << (i % 8);
                    if let Some(dict) = categories {
                        self.categories
                            .extend_from_slice(&id.to_le_bytes()[..dict.width]);
                    } else {
                        plain_string(&mut self.categories, &dimensions.categories[*id as usize])?;
                    }
                }
            }
            self.categories.resize(self.categories.len() + pad, 0);
            Ok(end)
        }
    }

    struct Writer {
        file: BufWriter<File>,
        pos: i64,
        encoder: Encoder,
        compressed: Vec<u8>,
        header: Vec<u8>,
        groups: Vec<f::RowGroup>,
        rows: i64,
    }

    impl Writer {
        fn open(path: &Path) -> Result<Self> {
            let mut file = BufWriter::with_capacity(options().buffer, File::create(path)?);
            file.write_all(b"PAR1")?;
            Ok(Self {
                file,
                pos: 4,
                encoder: Encoder::new(),
                compressed: Vec::new(),
                header: Vec::new(),
                groups: Vec::new(),
                rows: 0,
            })
        }

        fn page(
            &mut self,
            raw: &[u8],
            num_values: i32,
            encoding: f::Encoding,
            dictionary: bool,
            codec: f::CompressionCodec,
        ) -> Result<i64> {
            let raw_size = i32::try_from(raw.len())?;
            let body = if codec == f::CompressionCodec::SNAPPY {
                let wanted = max_compress_len(raw.len());
                if self.compressed.len() < wanted {
                    self.compressed.resize(wanted, 0);
                }
                let written = self.encoder.compress(raw, &mut self.compressed[..wanted])?;
                &self.compressed[..written]
            } else {
                raw
            };
            let (kind, data, dict) = if dictionary {
                (
                    f::PageType::DICTIONARY_PAGE,
                    None,
                    Some(f::DictionaryPageHeader::new(
                        num_values,
                        f::Encoding::PLAIN,
                        None,
                    )),
                )
            } else {
                (
                    f::PageType::DATA_PAGE,
                    Some(f::DataPageHeader::new(
                        num_values,
                        encoding,
                        f::Encoding::RLE,
                        f::Encoding::RLE,
                        None,
                    )),
                    None,
                )
            };
            let page = f::PageHeader::new(
                kind,
                raw_size,
                i32::try_from(body.len())?,
                None,
                data,
                None,
                dict,
                None,
            );
            self.header.clear();
            page.write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut self.header))?;
            self.file.write_all(&self.header)?;
            self.file.write_all(body)?;
            self.pos += (self.header.len() + body.len()) as i64;
            Ok(self.header.len() as i64 + i64::from(raw_size))
        }

        fn column(
            &mut self,
            index: usize,
            count: usize,
            data: &[u8],
            dictionary: Option<(&[u8], usize)>,
        ) -> Result<f::ColumnChunk> {
            let largest = data.len().max(dictionary.map_or(0, |(raw, _)| raw.len()));
            let codec = if !options().plain
                && max_compress_len(largest) <= i32::MAX as usize
                && largest <= i32::MAX as usize
            {
                f::CompressionCodec::SNAPPY
            } else {
                f::CompressionCodec::UNCOMPRESSED
            };
            let first = self.pos;
            let mut uncompressed = 0;
            let mut encodings = vec![f::Encoding::PLAIN];
            let data_encoding = if let Some((values, unique_count)) = dictionary {
                uncompressed += self.page(
                    values,
                    i32::try_from(unique_count)?,
                    f::Encoding::PLAIN,
                    true,
                    codec,
                )?;
                encodings.push(f::Encoding::RLE_DICTIONARY);
                f::Encoding::RLE_DICTIONARY
            } else {
                f::Encoding::PLAIN
            };
            if index == 5 {
                encodings.push(f::Encoding::RLE);
            }
            let data_offset = self.pos;
            uncompressed += self.page(data, i32::try_from(count)?, data_encoding, false, codec)?;
            let (name, ty) = COLUMNS[index];
            let meta = f::ColumnMetaData {
                type_: ty,
                encodings,
                path_in_schema: vec![name.into()],
                codec,
                num_values: count as i64,
                total_uncompressed_size: uncompressed,
                total_compressed_size: self.pos - first,
                key_value_metadata: None,
                data_page_offset: data_offset,
                index_page_offset: None,
                dictionary_page_offset: dictionary.map(|_| first),
                statistics: None,
                encoding_stats: None,
                bloom_filter_offset: None,
                bloom_filter_length: None,
                size_statistics: None,
            };
            Ok(f::ColumnChunk::new(
                None,
                0,
                Some(meta),
                None,
                None,
                None,
                None,
                None,
                None,
            ))
        }

        fn group(
            &mut self,
            daily: &DailyColumns,
            start: usize,
            end: usize,
            group: &Group,
            numbers: &mut Numeric,
            categories: Option<&CategoryDictionary>,
        ) -> Result<()> {
            let count = end - start;
            let first = self.pos;
            let mut columns = Vec::with_capacity(6);
            columns.push(self.column(
                0,
                count,
                &group.indices,
                Some((&group.dictionary, group.runs.len())),
            )?);
            let days = &daily.day[start..end];
            let day = if numbers.day(days) {
                self.column(
                    1,
                    count,
                    &numbers.values,
                    Some((&numbers.dictionary, numbers.dictionary.len() / 4)),
                )?
            } else {
                self.column(1, count, &i32_bytes(days), None)?
            };
            columns.push(day);
            for (index, values) in [
                (2, &daily.sessions[start..end]),
                (3, &daily.events[start..end]),
            ] {
                let col = if numbers.count(values) {
                    self.column(
                        index,
                        count,
                        &numbers.values,
                        Some((&numbers.dictionary, numbers.dictionary.len() / 8)),
                    )?
                } else {
                    self.column(index, count, &i64_bytes(values), None)?
                };
                columns.push(col);
            }
            columns.push(self.column(4, count, &i64_bytes(&daily.revenue[start..end]), None)?);
            columns.push(self.column(
                5,
                count,
                &group.categories,
                categories.map(|d| (d.bytes.as_slice(), d.entries)),
            )?);
            let size = columns
                .iter()
                .map(|c| c.meta_data.as_ref().unwrap().total_uncompressed_size)
                .sum();
            self.groups.push(f::RowGroup::new(
                columns,
                size,
                count as i64,
                None,
                Some(first),
                Some(self.pos - first),
                None,
            ));
            self.rows += count as i64;
            Ok(())
        }

        fn close(mut self) -> Result<()> {
            let footer = f::FileMetaData::new(
                1,
                schema(),
                self.rows,
                self.groups,
                None,
                Some("shopping-sessions".to_owned()),
                None,
                None,
                None,
            );
            self.header.clear();
            footer.write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut self.header))?;
            self.file.write_all(&self.header)?;
            self.file
                .write_all(&u32::try_from(self.header.len())?.to_le_bytes())?;
            self.file.write_all(b"PAR1")?;
            self.file.flush()?;
            Ok(())
        }
    }

    pub(super) fn users(
        output: &Path,
        part: usize,
        shards: Vec<SessionBatch>,
        names: &PersonNames,
        dimensions: &Dimensions,
        dictionary: Option<&PreparedNames>,
    ) -> Result<()> {
        let directory = output.join("user_daily");
        fs::create_dir_all(&directory)?;
        let mut writer = Writer::open(&directory.join(format!("part-{part}.parquet")))?;
        let mut group = Group::default();
        let mut numbers = Numeric::default();
        let categories = CategoryDictionary::build(&dimensions.categories)?;
        let mut columns: Vec<_> = shards.into_iter().map(|s| s.daily).collect();
        if options().merge && columns.len() > 1 {
            let mut merged = DailyColumns::default();
            let n = columns.iter().map(|s| s.person.len()).sum();
            if columns
                .iter()
                .all(|s| s.prepared_names.is_some() || s.person.is_empty())
            {
                let mut dictionary = PreparedNames::default();
                for part in &mut columns {
                    if let Some(d) = part.prepared_names.take() {
                        dictionary.append(d);
                    }
                }
                merged.prepared_names = Some(dictionary);
            }
            macro_rules! append_column {
                ($field:ident) => {{
                    merged.$field.reserve_exact(n);
                    for part in &mut columns {
                        merged.$field.append(&mut part.$field);
                    }
                }};
            }
            append_column!(person);
            append_column!(day);
            append_column!(sessions);
            append_column!(events);
            append_column!(revenue);
            append_column!(category);
            columns = vec![merged];
        }
        let measure = crate::csv::work_enabled();
        let (mut aggregate_name_rows, mut aggregate_name_runs, mut aggregate_name_bytes) =
            (0, 0, 0);
        let mut prepare_ns = 0u64;
        let mut encode_write_ns = 0u64;
        let mut base = 0;
        let mut held = Vec::new();
        let hold = crate::runtime::flag("PERF_RELEASE_AFTER_CLOSE");
        for daily in columns {
            if measure {
                if let Some(prepared) = &daily.prepared_names {
                    aggregate_name_rows += daily.person.len();
                    aggregate_name_runs += prepared.runs.len();
                    aggregate_name_bytes += prepared.bytes.len();
                }
            }
            crate::runtime::huge_pages(&daily.person);
            let mut start = 0;
            while start < daily.person.len() {
                let t = measure.then(std::time::Instant::now);
                let end = group.prepare_rows(
                    &daily,
                    start,
                    names,
                    dimensions,
                    categories.as_ref(),
                    daily.prepared_names.as_ref().or(dictionary),
                    if daily.prepared_names.is_some() {
                        0
                    } else {
                        base
                    },
                )?;
                if let Some(t) = t {
                    prepare_ns += t.elapsed().as_nanos() as u64;
                }
                let t = measure.then(std::time::Instant::now);
                writer.group(
                    &daily,
                    start,
                    end,
                    &group,
                    &mut numbers,
                    categories.as_ref(),
                )?;
                if let Some(t) = t {
                    encode_write_ns += t.elapsed().as_nanos() as u64;
                }
                start = end;
            }
            base += daily.person.len();
            if hold {
                held.push(daily);
            }
        }
        let groups = writer.groups.len();
        writer.close()?;
        if measure {
            eprintln!(
                "profile {}",
                serde_json::json!({"stage":"user_writer_work","part":part,
                "rows":base,"row_groups":groups,"prepare_ns":prepare_ns,"encode_write_ns":encode_write_ns,
                "aggregate_name_rows":aggregate_name_rows,"aggregate_name_runs":aggregate_name_runs,
                "aggregate_name_bytes":aggregate_name_bytes})
            );
        }
        drop(held);
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use arrow_array::{Array, RecordBatchReader};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        #[test]
        fn direct_pages_round_trip_dict_runs_levels_dates_and_empty_files() -> Result<()> {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let dir = std::env::temp_dir().join(format!(
                "shopping-direct-parquet-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&dir)?;
            fs::write(dir.join("products.csv"), "product_id,category\n")?;
            fs::write(dir.join("campaigns.csv"), "campaign_id,channel\n")?;
            let mut dims = Dimensions::read(&dir)?;
            dims.categories = vec![String::new(), "café🛒".into(), "a\0b".into()];
            let mut registry = crate::input::Names::default();
            let aliases = crate::input::read_aliases(&dir, &mut registry)?;
            let labels: Vec<String> = (0..300).map(|i| format!("ユーザー\0{i}")).collect();
            let ids: Vec<_> = labels.iter().map(|s| registry.intern(s)).collect();
            drop(aliases);
            // Sessionization emits one name/run per whole person even when
            // that person's sessions occupy several daily rows.
            let aggregate = |people: &[u32]| {
                let rows = people
                    .iter()
                    .enumerate()
                    .map(|(i, &person)| crate::records::Event {
                        person,
                        time: i as i64 * crate::input::DAY,
                        purchase: 0,
                    })
                    .collect();
                crate::aggregate::sessionize(
                    crate::records::Batch {
                        rows,
                        purchases: Vec::new(),
                    },
                    Some(&registry),
                )
                .daily
            };
            let mut initial = aggregate(&[ids[0], ids[0]]);
            let mut later = aggregate(&[ids[1], ids[1], ids[1], ids[2]]);
            let mut combined = initial.prepared_names.take().unwrap();
            combined.append(aggregate(&[]).prepared_names.unwrap());
            combined.append(later.prepared_names.take().unwrap());
            assert_eq!(
                combined.runs.iter().map(|r| r.0).collect::<Vec<_>>(),
                [2, 5, 6]
            );
            let names = registry.into_people();
            // A new row group can start midway through a run in a later shard.
            for start in 0..later.person.len() {
                let mut reference = Group::default();
                let mut actual = Group::default();
                let end = reference.prepare(&later, start, &names, &dims, None)?;
                assert_eq!(
                    actual.prepare_rows(&later, start, &names, &dims, None, Some(&combined), 2)?,
                    end
                );
                assert_eq!(actual.dictionary, reference.dictionary);
                assert_eq!(actual.indices, reference.indices);
            }
            let path = dir.join("values.parquet");
            let mut writer = Writer::open(&path)?;
            let mut group = Group::default();
            let mut numbers = Numeric::default();
            let categories = CategoryDictionary::build(&dims.categories)?;
            let mut expected = Vec::new();
            for n in [300, 1] {
                let mut daily = DailyColumns::default();
                for i in 0..n {
                    for k in 0..1 + i % 9 {
                        let cat = if n == 1 || k % 4 == 3 {
                            None
                        } else {
                            Some((k % 3) as u32)
                        };
                        let day = i as i32 % 17 - 17000;
                        let sessions = i as i64 % 17 + 1 + if i % 41 == 0 { 1i64 << 42 } else { 0 };
                        let events = sessions + k as i64;
                        let revenue = (1i64 << 43) + events;
                        daily.person.push(ids[i]);
                        daily.day.push(day);
                        daily.sessions.push(sessions);
                        daily.events.push(events);
                        daily.revenue.push(revenue);
                        daily.category.push(cat);
                        expected.push((
                            labels[i].clone(),
                            day,
                            sessions,
                            events,
                            revenue,
                            cat.map(|v| dims.categories[v as usize].clone()),
                        ));
                    }
                }
                let end = group.prepare(&daily, 0, &names, &dims, categories.as_ref())?;
                writer.group(&daily, 0, end, &group, &mut numbers, categories.as_ref())?;
            }
            writer.close()?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?
                .with_batch_size(17)
                .build()?;
            let mut actual = Vec::new();
            for batch in reader {
                let batch = batch?;
                let people = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let days = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap();
                let integer = |col| {
                    batch
                        .column(col)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                };
                let cats = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    actual.push((
                        people.value(i).to_owned(),
                        days.value(i),
                        integer(2).value(i),
                        integer(3).value(i),
                        integer(4).value(i),
                        (!cats.is_null(i)).then(|| cats.value(i).to_owned()),
                    ));
                }
            }
            assert_eq!(actual, expected);
            let empty = dir.join("empty.parquet");
            Writer::open(&empty)?.close()?;
            let mut reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&empty)?)?.build()?;
            assert_eq!(reader.schema().field(1).data_type(), &DataType::Date32);
            assert!(reader.next().is_none());
            drop(reader);
            fs::remove_file(path)?;
            fs::remove_file(empty)?;
            fs::remove_file(dir.join("products.csv"))?;
            fs::remove_file(dir.join("campaigns.csv"))?;
            fs::remove_dir(dir)?;
            Ok(())
        }
    }
}
