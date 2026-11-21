use arrow_array::builder::StringBuilder;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::{Array, ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::aggregate::{CampaignDays, SessionBatch};
use crate::input::{Dimensions, PersonNames};

const BATCH_SIZE: usize = 65_536;
const USER_PARTS: usize = 8;

pub fn write(
    output: &Path,
    users: Vec<SessionBatch>,
    campaigns: CampaignDays,
    names: &PersonNames,
    dimensions: &Dimensions,
) -> Result<()> {
    let (user_result, campaign_result) = rayon::join(
        || {
            let shards = users.len();
            let mut parts: [Vec<_>; USER_PARTS] = std::array::from_fn(|_| Vec::new());
            for (i, shard) in users.into_iter().enumerate() {
                parts[i * USER_PARTS / shards].push(shard);
            }
            parts
                .into_par_iter()
                .enumerate()
                .try_for_each(|(part, batches)| {
                    write_users(output, part, batches, names, dimensions)
                })
        },
        || write_campaigns(output, campaigns, dimensions),
    );
    user_result?;
    campaign_result
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
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(BATCH_SIZE)
        .build();
    let writer = ArrowWriter::try_new(
        File::create(directory.join(format!("part-{part}.parquet")))?,
        schema.clone(),
        Some(properties),
    )?;
    Ok((schema, writer))
}

fn write_users(
    output: &Path,
    part: usize,
    users: Vec<SessionBatch>,
    names: &PersonNames,
    dimensions: &Dimensions,
) -> Result<()> {
    let (schema, mut writer) = open(
        output,
        "user_daily",
        part,
        &[
            ("user_id", DataType::Utf8, false),
            ("day", DataType::Date32, false),
            ("sessions", DataType::Int64, false),
            ("events", DataType::Int64, false),
            ("revenue_cents", DataType::Int64, false),
            ("top_category", DataType::Utf8, true),
        ],
    )?;
    for shard in users {
        let daily = shard.daily;
        // Primitive Arrow arrays take ownership of these four contiguous
        // buffers. Taking a row-group slice only adjusts offsets and lengths.
        let primitives: [ArrayRef; 4] = [
            Arc::new(Date32Array::from(daily.day)),
            Arc::new(Int64Array::from(daily.sessions)),
            Arc::new(Int64Array::from(daily.events)),
            Arc::new(Int64Array::from(daily.revenue)),
        ];
        for (chunk, people) in daily.person.chunks(BATCH_SIZE).enumerate() {
            let offset = chunk * BATCH_SIZE;
            let len = people.len();
            let mut user_ids = StringBuilder::with_capacity(len, len * 9);
            let mut buffer = [0; 9];
            for group in people.chunk_by(|a, b| a == b) {
                let name = names.get(group[0], &mut buffer);
                for _ in group {
                    user_ids.append_value(name);
                }
            }
            let categories = StringArray::from_iter(
                daily.category[offset..offset + len]
                    .iter()
                    .map(|id| id.map(|id| dimensions.categories[id as usize].as_str())),
            );
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(6);
            columns.push(Arc::new(user_ids.finish()));
            columns.extend(primitives.iter().map(|values| values.slice(offset, len)));
            columns.push(Arc::new(categories));
            writer.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
        }
    }
    writer.close()?;
    Ok(())
}

fn write_campaigns(output: &Path, campaigns: CampaignDays, dimensions: &Dimensions) -> Result<()> {
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
    let rows: Vec<_> = campaigns.into_iter().collect();
    for rows in rows.chunks(BATCH_SIZE) {
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
    writer.close()?;
    Ok(())
}
