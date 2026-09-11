// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker warning.
#![cfg_attr(
    all(target_os = "macos", debug_assertions),
    allow(
        linker_messages,
        reason = "Apple ld falls back to DWARF when the CLI exceeds compact unwind's 16 MiB offset range; rust-lang/rust#159105 tracks this diagnostic"
    )
)]

use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::sync::Arc;

use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

#[tokio::main]
async fn main() -> Result<()> {
    let format = Arc::new(VortexFormat::new(VortexSession::default()));
    let ctx = SessionContext::new();
    let vortex_opts = ListingOptions::new(format);
    ctx.register_listing_table(
        "ref",
        "data/NA20760.hg38.g.reference.vortex",
        vortex_opts.clone(),
        None,
        None,
    )
    .await?;
    let df = ctx.table("ref").await?;
    let df = df.sort_by(vec![
        col(Column::from_name("locus.contig")),
        col(Column::from_name("locus.position")),
    ])?;
    ctx.register_listing_table(
        "out",
        "data/test/",
        vortex_opts,
        Some(Arc::new(df.schema().as_arrow().clone())),
        None,
    )
    .await?;
    df.limit(0, Some(30))?.show().await?;
    // df.explain(false, false)?.show().await?;
    // df.write_table("out", DataFrameWriteOptions::new()).await?;
    Ok(())
}
