use arrow_array::builder::StringBuilder;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

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

pub fn write(
    output: &Path,
    users: Vec<SessionBatch>,
    campaigns: CampaignDays,
    names: &PersonNames,
    dimensions: &Dimensions,
) -> Result<()> {
    let (user_result, campaign_result) = rayon::join(
        || {
            (0..USER_PARTS).into_par_iter().try_for_each(|part| {
                let batches =
                    &users[part * users.len() / USER_PARTS..(part + 1) * users.len() / USER_PARTS];
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
    users: &[SessionBatch],
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
    for batch in users {
        for rows in batch.daily.chunks(BATCH_SIZE) {
            let mut user_ids = StringBuilder::with_capacity(rows.len(), rows.len() * 9);
            let mut name = [0; 9];
            for row in rows {
                user_ids.append_value(names.get(row.person, &mut name));
            }
            let user_id = user_ids.finish();
            let category = StringArray::from_iter(rows.iter().map(|row| {
                row.top_category
                    .map(|id| dimensions.categories[id as usize].as_str())
            }));
            let columns: Vec<ArrayRef> = vec![
                Arc::new(user_id),
                Arc::new(Date32Array::from_iter_values(
                    rows.iter().map(|row| row.day),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|row| row.sessions),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|row| row.events),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|row| row.revenue),
                )),
                Arc::new(category),
            ];
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
