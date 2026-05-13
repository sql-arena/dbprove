use anyhow::{anyhow, bail, Result};
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::env;
use std::fs;

const TPCH_TABLES: &[&str] = &[
  "region",
  "nation",
  "supplier",
  "customer",
  "part",
  "partsupp",
  "orders",
  "lineitem",
];

async fn register_tpch(ctx: &SessionContext) -> Result<()> {
  ctx.sql("CREATE SCHEMA IF NOT EXISTS tpch").await?.collect().await?;
  for table in TPCH_TABLES {
    let statement = format!(
      "CREATE EXTERNAL TABLE IF NOT EXISTS tpch.{table} STORED AS PARQUET LOCATION '/opt/tpch/sf1/{table}.parquet'"
    );
    ctx.sql(&statement).await?.collect().await?;
  }
  Ok(())
}

fn parse_args() -> Result<String> {
  let mut args = env::args().skip(1);
  let mut sql = None;

  while let Some(arg) = args.next() {
    match arg.as_str() {
      "--sql" => sql = args.next(),
      "--sql-file" => {
        let path = args.next().ok_or_else(|| anyhow::anyhow!("missing value for --sql-file"))?;
        sql = Some(fs::read_to_string(path)?);
      }
      other => bail!("unsupported argument: {other}"),
    }
  }

  sql.ok_or_else(|| anyhow::anyhow!("provide --sql or --sql-file"))
}

fn precision_to_json(value: &Precision<usize>) -> Value {
  match value {
    Precision::Exact(value) => json!({
      "value": value,
      "precision": "exact"
    }),
    Precision::Inexact(value) => json!({
      "value": value,
      "precision": "inexact"
    }),
    Precision::Absent => json!({
      "precision": "absent"
    }),
  }
}

fn statistics_to_json(statistics: &Statistics) -> Value {
  json!({
    "numRows": precision_to_json(&statistics.num_rows),
    "totalByteSize": precision_to_json(&statistics.total_byte_size)
  })
}

fn metrics_to_json(plan: &Arc<dyn ExecutionPlan>) -> Option<Value> {
  plan.metrics().map(|metrics| {
    let mut payload = Map::new();

    if let Some(output_rows) = metrics.output_rows() {
      payload.insert("outputRows".to_string(), json!(output_rows));
    }

    for metric in metrics.iter() {
      payload
        .entry(metric.value().name().to_string())
        .or_insert_with(|| json!(metric.value().to_string()));
    }

    Value::Object(payload)
  })
}

fn annotate_node(wrapper: &mut Value, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
  let object = wrapper
    .as_object_mut()
    .ok_or_else(|| anyhow!("physical plan JSON node was not an object"))?;
  let (_, body) = object
    .iter_mut()
    .next()
    .ok_or_else(|| anyhow!("physical plan JSON node was empty"))?;
  let body_object = body
    .as_object_mut()
    .ok_or_else(|| anyhow!("physical plan JSON body was not an object"))?;

  let statistics = plan.partition_statistics(None)?;
  let mut dbprove = Map::new();
  dbprove.insert("statistics".to_string(), statistics_to_json(&statistics));

  if let Some(metrics) = metrics_to_json(plan) {
    dbprove.insert("metrics".to_string(), metrics);
  }

  body_object.insert("dbprove".to_string(), Value::Object(dbprove));

  let children = plan.children();
  match children.as_slice() {
    [] => {}
    [child] => {
      if let Some(input) = body_object.get_mut("input") {
        annotate_node(input, child)?;
      }
    }
    [left, right] => {
      if let Some(left_json) = body_object.get_mut("left") {
        annotate_node(left_json, left)?;
      }
      if let Some(right_json) = body_object.get_mut("right") {
        annotate_node(right_json, right)?;
      }
    }
    _ => {
      if let Some(inputs) = body_object.get_mut("inputs").and_then(Value::as_array_mut) {
        for (child_json, child_plan) in inputs.iter_mut().zip(children.iter()) {
          annotate_node(child_json, child_plan)?;
        }
      }
    }
  }

  Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
  let sql = parse_args()?;
  let ctx = SessionContext::new();
  register_tpch(&ctx).await?;

  let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
  collect(plan.clone(), ctx.task_ctx()).await?;

  let mut json: Value = serde_json::from_str(&datafusion_proto::bytes::physical_plan_to_json(plan.clone())?)?;
  annotate_node(&mut json, &plan)?;

  println!("{}", serde_json::to_string(&json)?);
  Ok(())
}
