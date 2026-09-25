// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker warning.
#![cfg_attr(
    all(target_os = "macos", debug_assertions),
    allow(
        linker_messages,
        reason = "Apple ld falls back to DWARF when the CLI exceeds compact unwind's 16 MiB offset range; rust-lang/rust#159105 tracks this diagnostic"
    )
)]

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use datafusion::{
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
};

use datafusion_sandbox::combiner_run::{Action, CombinerRun};
use datafusion_sandbox::format::{InputFormat, OutputFormat};
use datafusion_sandbox::formulation::Formulation;
use datafusion_sandbox::locus::SplitPoints;
use datafusion_sandbox::metrics_directory::MetricsDirectory;
use datafusion_sandbox::ordered_frame::OutputLayout;
use datafusion_sandbox::pipeline::{self, PipelineOptions};
use datafusion_sandbox::split_points;
use datafusion_sandbox::throughput_probe::ProbeSettings;
use datafusion_sandbox::write::WriteTarget;
use std::{
    num::{NonZeroU32, NonZeroUsize},
    path::Path,
    time::Duration,
};
use uuid::Uuid;

const DEFAULT_SHOW_LIMIT: usize = 20;

#[derive(Parser)]
#[command(about = "Run hail-style pipelines built on datafusion")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Number of worker threads to execute on.
    ///
    /// Defaults to available parallelism. Independent of the session's target-partition setting.
    #[arg(long, short = 'j', global = true, value_parser = parse_thread_count)]
    threads: Option<NonZeroUsize>,
}

/// Rejects 0 with a message rather than clap's generated
/// `0 is not in 1..18446744073709551615`.
fn parse_thread_count(value: &str) -> std::result::Result<NonZeroUsize, String> {
    match value.parse::<usize>() {
        Ok(threads) => {
            NonZeroUsize::new(threads).ok_or_else(|| "thread count must be at least 1".to_string())
        }
        Err(e) => Err(e.to_string()),
    }
}

/// A precision target, which must be positive and finite.
fn parse_precision(value: &str) -> std::result::Result<f64, String> {
    value
        .parse::<f64>()
        .ok()
        .filter(|precision| *precision > 0.0 && precision.is_finite())
        .ok_or_else(|| format!("expected a positive, finite fraction, got '{value}'"))
}

/// A duration given in decimal seconds, which must be positive and finite.
fn parse_seconds(value: &str) -> std::result::Result<Duration, String> {
    value
        .parse::<f64>()
        .ok()
        .filter(|seconds| *seconds > 0.0)
        .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
        .ok_or_else(|| format!("expected a positive, finite number of seconds, got '{value}'"))
}

fn parse_interval_count(value: &str) -> std::result::Result<NonZeroUsize, String> {
    match value.parse::<usize>() {
        Ok(intervals) if intervals >= 2 => NonZeroUsize::new(intervals)
            .ok_or_else(|| "interval count must be at least 2".to_string()),
        Ok(_) => Err("interval count must be at least 2".to_string()),
        Err(error) => Err(error.to_string()),
    }
}

#[derive(Subcommand)]
enum Command {
    /// Combine reference data for the dataset's sample set under PATH.
    CombineRefs(CombineRefsArgs),
    /// Combine alleles for the dataset's sample set under PATH.
    CombineAlleles(CombinerArgs),
    /// Compute row-balanced split points from a locus-sorted table.
    BalanceSplitPoints(BalanceSplitPointsArgs),
}

#[derive(Args)]
struct BalanceSplitPointsArgs {
    path: String,
    /// Number of locus intervals the printed split points define.
    #[arg(long, value_name = "J", value_parser = parse_interval_count)]
    intervals: NonZeroUsize,
    /// Format of the input table.
    #[arg(long, value_enum, default_value = "vortex")]
    input_format: InputFormatArg,
}

#[derive(Args)]
struct CombineRefsArgs {
    #[command(flatten)]
    combiner: CombinerArgs,
    /// How to build the reference combiner's plan.
    #[arg(long, value_enum, default_value = "union")]
    formulation: CombineRefsFormulationArg,
    /// Number of sample groups the grouped-merge formulation merges before merging the groups.
    ///
    /// Defaults to the thread count. One fewer gives the final merge a thread of its own.
    /// Accepted only with `--formulation grouped-merge`.
    #[arg(long, value_name = "N")]
    groups: Option<NonZeroUsize>,
    /// The loci cutting the locus ordering into the intervals the interval-merge formulation
    /// merges, one file each: comma-separated `contig:position`, strictly increasing, where
    /// `contig` is the contig ordinal. Required by, and accepted only with,
    /// `--formulation interval-merge`.
    #[arg(long, value_name = "CONTIG:POSITION,...")]
    split_points: Option<SplitPoints>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CombineRefsFormulationArg {
    Union,
    GroupedMerge,
    IntervalMerge,
}

impl CombineRefsFormulationArg {
    /// The formulation for this argument, given `--groups`, `--split-points`, and the run's
    /// thread count.
    fn formulation(
        self,
        groups: Option<NonZeroUsize>,
        split_points: Option<SplitPoints>,
        threads: NonZeroUsize,
    ) -> Result<Formulation> {
        if groups.is_some() && self != Self::GroupedMerge {
            return Err(DataFusionError::Configuration(
                "--groups applies only to the grouped-merge formulation".to_string(),
            ));
        }
        if split_points.is_some() && self != Self::IntervalMerge {
            return Err(DataFusionError::Configuration(
                "--split-points applies only to the interval-merge formulation".to_string(),
            ));
        }
        match self {
            Self::Union => Ok(Formulation::CombineRefsUnion),
            Self::GroupedMerge => Ok(Formulation::CombineRefsGroupedMerge {
                groups: groups.unwrap_or(threads),
            }),
            Self::IntervalMerge => split_points
                .map(|split_points| Formulation::CombineRefsIntervalMerge { split_points })
                .ok_or_else(|| {
                    DataFusionError::Configuration(
                        "the interval-merge formulation requires --split-points".to_string(),
                    )
                }),
        }
    }
}

#[derive(Args)]
struct CombinerArgs {
    path: String,
    #[command(flatten)]
    formats: FormatArgs,
    #[command(flatten)]
    action: ActionArgs,
    /// Compression to use when writing.
    ///
    /// Parquet accepts `uncompressed`, `snappy`, `gzip(LEVEL)`, `brotli(LEVEL)`, `lz4`,
    /// `zstd(LEVEL)`, or `lz4_raw`. Vortex accepts `standard` or `compact`.
    // The explicit conflict with `--show` is needed: clap drops a `requires` whose target
    // conflicts with a present argument, so `--compression --show` would otherwise parse.
    #[arg(
        long,
        value_name = "COMPRESSION",
        requires = "write",
        conflicts_with = "show"
    )]
    compression: Option<String>,
    /// Record the run under DIR: its run record at DIR/runs/<ID>.parquet and its run metrics at
    /// DIR/metrics/<ID>.parquet, and a probe's progress samples at DIR/progress/<ID>.parquet, all
    /// Parquet whatever the output format. DIR may be any path --write accepts. Requires --probe,
    /// or --write, whose write it measures.
    #[arg(
        long,
        value_name = "DIR",
        requires = "recorded_action",
        conflicts_with_all = ["show", "explain", "explain_analyze"]
    )]
    metrics: Option<String>,
    /// The id naming this run in the metrics tables. Defaults to a generated UUID.
    #[arg(long, value_name = "ID", requires = "metrics")]
    run_id: Option<String>,
    #[command(flatten)]
    probe_settings: ProbeArgs,
    /// Return at most ROWS combined rows. Defaults to 20 with --show and unlimited otherwise.
    #[arg(long, value_name = "ROWS")]
    limit: Option<usize>,
    /// Restrict the dataset's sample set to comma-separated sample ids.
    #[arg(long = "samples", value_delimiter = ',', value_name = "SAMPLE,...")]
    sample_set: Vec<String>,
}

#[derive(Args)]
struct FormatArgs {
    /// Format of the input tables.
    #[arg(long, value_enum, default_value = "vortex")]
    input_format: InputFormatArg,
    /// Format to write. Defaults to the input format.
    #[arg(long, value_enum)]
    output_format: Option<OutputFormatArg>,
}

impl FormatArgs {
    fn output_format(&self) -> OutputFormatArg {
        self.output_format
            .unwrap_or_else(|| self.input_format.default_output_format())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum InputFormatArg {
    Parquet,
    Vortex,
}

impl InputFormatArg {
    const fn format(self) -> InputFormat {
        match self {
            Self::Parquet => InputFormat::PARQUET,
            Self::Vortex => InputFormat::VORTEX,
        }
    }

    const fn default_output_format(self) -> OutputFormatArg {
        match self {
            Self::Parquet => OutputFormatArg::Parquet,
            Self::Vortex => OutputFormatArg::Vortex,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormatArg {
    Parquet,
    Vortex,
}

impl OutputFormatArg {
    const fn format(self) -> OutputFormat {
        match self {
            Self::Parquet => OutputFormat::PARQUET,
            Self::Vortex => OutputFormat::VORTEX,
        }
    }
}

/// The settings of a throughput probe, each of which requires `--probe`. Durations are in decimal
/// seconds.
// Every conflict of `--probe` is repeated on each: clap drops a `requires` whose target
// conflicts with a present argument, so `--max-duration --limit` would otherwise parse.
#[derive(Args)]
struct ProbeArgs {
    /// With --probe, take a progress sample every SECONDS. Defaults to 0.1.
    #[arg(
        long,
        value_name = "SECONDS",
        value_parser = parse_seconds,
        requires = "probe",
        conflicts_with_all = ["show", "explain", "explain_analyze", "limit"]
    )]
    poll_period: Option<Duration>,
    /// With --probe, batch progress samples into batches of at least SECONDS, over which MSER
    /// judges the end of warmup and after each of which the rule checks its interval. Defaults
    /// to 1.
    #[arg(
        long,
        value_name = "SECONDS",
        value_parser = parse_seconds,
        requires = "probe",
        conflicts_with_all = ["show", "explain", "explain_analyze", "limit"]
    )]
    batch: Option<Duration>,
    /// With --probe, count a check as tight when its interval's half-width is below FRACTION of
    /// its estimate. Defaults to 0.02.
    #[arg(
        long,
        value_name = "FRACTION",
        value_parser = parse_precision,
        requires = "probe",
        conflicts_with_all = ["show", "explain", "explain_analyze", "limit"]
    )]
    precision: Option<f64>,
    /// With --probe, stop, steady, after COUNT consecutive tight checks. Defaults to 3.
    #[arg(
        long,
        value_name = "COUNT",
        value_parser = clap::value_parser!(NonZeroU32),
        requires = "probe",
        conflicts_with_all = ["show", "explain", "explain_analyze", "limit"]
    )]
    consecutive: Option<NonZeroU32>,
    /// With --probe, split the measurement window into COUNT groups of equal duration for its
    /// interval, from 2 to 1000. Defaults to 10.
    #[arg(
        long,
        value_name = "COUNT",
        value_parser = clap::value_parser!(u32).range(2..=1000),
        requires = "probe",
        conflicts_with_all = ["show", "explain", "explain_analyze", "limit"]
    )]
    window_groups: Option<u32>,
    /// With --probe, stop steady no sooner than SECONDS of execution. Defaults to 20.
    #[arg(
        long,
        value_name = "SECONDS",
        value_parser = parse_seconds,
        requires = "probe",
        conflicts_with_all = ["show", "explain", "explain_analyze", "limit"]
    )]
    min_duration: Option<Duration>,
    /// With --probe, stop the probe, capped, once it has executed for SECONDS, unless the same
    /// progress sample stops it steady. Defaults to 300.
    #[arg(
        long,
        value_name = "SECONDS",
        value_parser = parse_seconds,
        requires = "probe",
        conflicts_with_all = ["show", "explain", "explain_analyze", "limit"]
    )]
    max_duration: Option<Duration>,
}

impl ProbeArgs {
    /// The probe settings, each at its default unless given.
    fn settings(self) -> ProbeSettings {
        let defaults = ProbeSettings::default();
        ProbeSettings {
            poll_period: self.poll_period.unwrap_or(defaults.poll_period),
            batch_duration: self.batch.unwrap_or(defaults.batch_duration),
            precision: self.precision.unwrap_or(defaults.precision),
            consecutive_checks: self.consecutive.unwrap_or(defaults.consecutive_checks),
            window_groups: self.window_groups.unwrap_or(defaults.window_groups),
            min_duration: self.min_duration.unwrap_or(defaults.min_duration),
            max_duration: self.max_duration.unwrap_or(defaults.max_duration),
        }
    }
}

/// The action flags. At least one is required. `--explain` and `--explain-analyze` may combine
/// with `--write`, in which case they render or analyze the write's plan, and `--probe` may too,
/// in which case it probes the write; every other pair conflicts. `--write` and `--probe` are
/// the recorded actions, one of which `--metrics` requires.
// The recorded-action group allows both, since a probe may take a write. A group whose members
// conflict would also drop `--compression`'s requirement of `--write` beside `--probe`.
#[derive(Args)]
#[group(required = true, multiple = true)]
#[command(group(ArgGroup::new("recorded_action").multiple(true)))]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is a clap switch, and `CliAction` resolves them to one action"
)]
struct ActionArgs {
    /// Write the combined rows to PATH. With --explain or --explain-analyze, the plan shown is
    /// the write's, and --explain-analyze performs the write. With --probe, the probe writes
    /// them, and keeps nothing.
    #[arg(long, value_name = "PATH", group = "recorded_action")]
    write: Option<String>,
    /// Print the combined rows.
    #[arg(long, conflicts_with_all = ["write", "explain", "explain_analyze"])]
    show: bool,
    /// Print the logical and physical plan without executing it.
    #[arg(long, conflicts_with = "explain_analyze")]
    explain: bool,
    /// Execute the plan and print it with per-operator metrics.
    #[arg(long)]
    explain_analyze: bool,
    /// Run the combined rows as a throughput probe, taking a progress sample every
    /// --poll-period, until its steady-state throughput settles, a partition of the plan
    /// finishes, or --max-duration passes. With --write, writes them to PATH, which must not
    /// exist, and removes everything under PATH afterwards; otherwise drains them. Records the
    /// run and its progress samples under --metrics, which it requires, and prints its
    /// steady-state throughput and stop reason.
    // The explicit conflicts matter: clap drops a `requires` whose target conflicts with a
    // present argument, so each conflict of `--metrics` is stated here too.
    #[arg(
        long,
        group = "recorded_action",
        requires = "metrics",
        conflicts_with_all = ["show", "explain", "explain_analyze", "limit"]
    )]
    probe: bool,
}

enum CliAction {
    Write(String),
    Show,
    Explain { write: Option<String> },
    ExplainAnalyze { write: Option<String> },
    Probe { write: Option<String> },
}

impl CliAction {
    fn row_limit(&self, explicit_limit: Option<usize>) -> Option<usize> {
        explicit_limit.or_else(|| matches!(self, Self::Show).then_some(DEFAULT_SHOW_LIMIT))
    }
}

impl TryFrom<ActionArgs> for CliAction {
    type Error = DataFusionError;

    fn try_from(args: ActionArgs) -> Result<Self> {
        let ActionArgs {
            write,
            show,
            explain,
            explain_analyze,
            probe,
        } = args;
        match (write, show, explain, explain_analyze, probe) {
            (Some(path), false, false, false, false) => Ok(Self::Write(path)),
            (None, true, false, false, false) => Ok(Self::Show),
            (write, false, true, false, false) => Ok(Self::Explain { write }),
            (write, false, false, true, false) => Ok(Self::ExplainAnalyze { write }),
            (write, false, false, false, true) => Ok(Self::Probe { write }),
            _ => Err(DataFusionError::Configuration(
                "an action is required: --write, --show, --explain, --explain-analyze, or \
                 --probe, where --explain, --explain-analyze and --probe may combine with --write"
                    .to_string(),
            )),
        }
    }
}

fn resolve(cli: Cli) -> Result<CombinerRun> {
    let Cli { command, threads } = cli;
    let threads = threads.unwrap_or_else(pipeline::default_thread_count);
    let (formulation, args) = match command {
        Command::CombineRefs(args) => (
            args.formulation
                .formulation(args.groups, args.split_points, threads)?,
            args.combiner,
        ),
        Command::CombineAlleles(args) => (Formulation::CombineAllelesUnion, args),
        Command::BalanceSplitPoints(_) => {
            return Err(DataFusionError::Internal(
                "balance-split-points was sent through the combiner resolver".to_string(),
            ));
        }
    };
    let CombinerArgs {
        path,
        formats,
        action,
        compression,
        metrics,
        run_id,
        probe_settings,
        limit,
        sample_set,
    } = args;
    let cli_action = CliAction::try_from(action)?;
    let input_format = formats.input_format.format();
    let limit = cli_action.row_limit(limit);
    let write_target = |output_path: String| -> Result<WriteTarget> {
        let output_format = formats.output_format().format();
        validate_output_path(&output_path, &output_format, &formulation)?;
        let output_format = match &compression {
            Some(compression) => output_format.with_compression(compression)?,
            None => output_format,
        };
        Ok(WriteTarget {
            output_path,
            output_format,
        })
    };
    let action = match cli_action {
        CliAction::Write(output_path) => {
            let write = write_target(output_path)?;
            match metrics {
                Some(metrics_directory) => Action::MeasuredWrite {
                    write,
                    metrics_directory: MetricsDirectory::new(&metrics_directory),
                    run_id: run_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                },
                None => Action::Write(write),
            }
        }
        CliAction::Probe { write } => Action::Probe {
            write: write.map(write_target).transpose()?,
            metrics_directory: MetricsDirectory::new(&metrics.ok_or_else(|| {
                DataFusionError::Configuration("--probe requires --metrics".to_string())
            })?),
            run_id: run_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            settings: probe_settings.settings(),
        },
        CliAction::Show => Action::Collect,
        CliAction::Explain { write } => Action::Explain {
            write: write.map(write_target).transpose()?,
        },
        CliAction::ExplainAnalyze { write } => Action::ExplainAnalyze {
            write: write.map(write_target).transpose()?,
        },
    };
    let sample_set = (!sample_set.is_empty()).then_some(sample_set);
    Ok(CombinerRun {
        formulation,
        input_path: path,
        input_format,
        action,
        sample_set,
        row_limit: limit,
        threads,
    })
}

fn main() -> Result<()> {
    let Cli { command, threads } = Cli::parse();
    match command {
        Command::BalanceSplitPoints(args) => {
            let threads = threads.unwrap_or_else(pipeline::default_thread_count);
            let input_format = args.input_format.format();
            validate_input_path(&args.path, &input_format)?;
            let options = PipelineOptions::for_paths(threads, [args.path.as_str()])?;
            let BalanceSplitPointsArgs {
                path, intervals, ..
            } = args;
            let points = pipeline::run(
                move |ctx| async move {
                    let table_path = ListingTableUrl::parse(path)?;
                    split_points::row_balanced(&ctx, table_path, input_format, intervals).await
                },
                options,
            )?;
            println!("{points}");
        }
        command => {
            let run = resolve(Cli { command, threads })?;
            println!("formulation: {}", run.formulation);
            if let Action::MeasuredWrite { run_id, .. } | Action::Probe { run_id, .. } = &run.action
            {
                println!("run id: {run_id}");
            }
            let outcome = run.execute()?;
            println!("{}", outcome.render()?);
        }
    }
    Ok(())
}

/// Rejects an input path whose recognized extension contradicts the selected input format.
fn validate_input_path(input_path: &str, input_format: &InputFormat) -> Result<()> {
    let Some(extension) = Path::new(input_path)
        .extension()
        .and_then(|extension| extension.to_str())
    else {
        return Ok(());
    };
    if matches!(extension, "parquet" | "vortex") && extension != input_format.name() {
        return Err(DataFusionError::Configuration(format!(
            "input path '{input_path}' has extension '.{extension}', which contradicts input format '{}'",
            input_format.name()
        )));
    }
    Ok(())
}

/// Rejects an output path whose extension contradicts the format, or any extension at all when
/// the path names the directory a file-per-partition formulation fills.
fn validate_output_path(
    output_path: &str,
    output_format: &OutputFormat,
    formulation: &Formulation,
) -> Result<()> {
    let Some(extension) = Path::new(output_path)
        .extension()
        .and_then(|ext| ext.to_str())
    else {
        return Ok(());
    };
    match formulation.output_layout() {
        OutputLayout::SingleFile if extension == output_format.extension() => Ok(()),
        OutputLayout::SingleFile => Err(DataFusionError::Configuration(format!(
            "output path '{output_path}' has extension '.{extension}', which contradicts output format '{output_format}'"
        ))),
        OutputLayout::FilePerPartition => Err(DataFusionError::Configuration(format!(
            "output path '{output_path}' has extension '.{extension}', but it names the directory \
             the {formulation} formulation writes one file per partition into"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balance_split_points_requires_at_least_two_intervals() {
        for intervals in ["0", "1"] {
            let error = Cli::try_parse_from([
                "datafusion-sandbox",
                "balance-split-points",
                "combined.vortex",
                "--intervals",
                intervals,
            ])
            .err()
            .unwrap();
            let diagnostic = error.to_string();
            assert!(
                diagnostic.contains("interval count must be at least 2"),
                "{intervals} diagnostic:\n{diagnostic}"
            );
        }
    }

    #[test]
    fn balance_split_points_defaults_to_vortex_and_honors_threads() {
        let cli = Cli::try_parse_from([
            "datafusion-sandbox",
            "--threads",
            "3",
            "balance-split-points",
            "combined",
            "--intervals",
            "4",
        ])
        .unwrap();

        assert_eq!(cli.threads, NonZeroUsize::new(3));
        let Command::BalanceSplitPoints(args) = cli.command else {
            panic!("expected balance-split-points");
        };
        assert_eq!(args.path, "combined");
        assert_eq!(args.intervals, NonZeroUsize::new(4).unwrap());
        assert_eq!(args.input_format, InputFormatArg::Vortex);
    }

    #[test]
    fn balance_split_points_rejects_a_contradictory_file_extension() {
        let error = validate_input_path("combined.parquet", &InputFormat::VORTEX)
            .expect_err("a Parquet extension must contradict the Vortex input format");

        assert!(
            error.to_string().contains(
                "input path 'combined.parquet' has extension '.parquet', which contradicts input format 'vortex'"
            ),
            "{error}"
        );
        validate_input_path("combined.vortex", &InputFormat::VORTEX)
            .expect("a Vortex extension agrees with the Vortex input format");
        validate_input_path("combined", &InputFormat::VORTEX)
            .expect("a path without a recognized extension is not format inference");
    }

    #[test]
    fn an_action_is_required() {
        let error = Cli::try_parse_from(["datafusion-sandbox", "combine-refs", "input"])
            .err()
            .unwrap();
        let diagnostic = error.to_string();

        for action in [
            "--write",
            "--show",
            "--explain",
            "--explain-analyze",
            "--probe",
        ] {
            assert!(diagnostic.contains(action), "diagnostic:\n{diagnostic}");
        }
    }

    #[test]
    fn show_conflicts_with_every_other_action_and_explain_with_explain_analyze() {
        for pair in [
            ["--show", "--explain"],
            ["--show", "--explain-analyze"],
            ["--show", "--write=out.vortex"],
            ["--explain", "--explain-analyze"],
        ] {
            let error = Cli::try_parse_from(
                ["datafusion-sandbox", "combine-alleles", "input"]
                    .into_iter()
                    .chain(pair),
            )
            .err()
            .unwrap();
            let diagnostic = error.to_string();

            assert!(
                diagnostic.contains("cannot be used with"),
                "{pair:?} diagnostic:\n{diagnostic}"
            );
        }
    }

    #[test]
    fn explain_and_explain_analyze_may_combine_with_write() {
        let run = resolve(parse_combiner(["--explain", "--write", "out.vortex"])).unwrap();
        let Action::Explain {
            write: Some(target),
        } = run.action
        else {
            panic!("expected an explained write");
        };
        assert_eq!(target.output_path, "out.vortex");
        assert_eq!(run.row_limit, None);

        let run = resolve(parse_combiner([
            "--explain-analyze",
            "--write",
            "out.parquet",
            "--output-format",
            "parquet",
            "--compression",
            "snappy",
        ]))
        .unwrap();
        let Action::ExplainAnalyze {
            write: Some(target),
        } = run.action
        else {
            panic!("expected an analyzed write");
        };
        assert_eq!(target.output_path, "out.parquet");
        assert_eq!(target.output_format.extension(), "parquet");
    }

    #[test]
    fn explain_actions_without_a_write_resolve_to_no_write_target() {
        let run = resolve(parse_combiner(["--explain"])).unwrap();
        assert!(matches!(run.action, Action::Explain { write: None }));

        let run = resolve(parse_combiner(["--explain-analyze"])).unwrap();
        assert!(matches!(run.action, Action::ExplainAnalyze { write: None }));
    }

    #[test]
    fn compression_requires_a_write_action() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-refs",
            "input",
            "--compression",
            "snappy",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("--compression"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(diagnostic.contains("--write"), "diagnostic:\n{diagnostic}");
    }

    /// `--show` conflicts with `--compression` outright; the explain actions may take a write,
    /// so there the diagnostic names the missing `--write`.
    #[test]
    fn compression_still_requires_a_write_under_each_non_write_action() {
        for (action, expected) in [
            ("--show", "cannot be used with"),
            ("--explain", "--write"),
            ("--explain-analyze", "--write"),
        ] {
            let error = Cli::try_parse_from([
                "datafusion-sandbox",
                "combine-refs",
                "input",
                "--compression",
                "snappy",
                action,
            ])
            .err()
            .unwrap();
            let diagnostic = error.to_string();

            assert!(
                diagnostic.contains(expected),
                "{action} diagnostic:\n{diagnostic}"
            );
        }
    }

    #[test]
    fn metrics_requires_a_write_or_a_probe() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-refs",
            "input",
            "--metrics",
            "runs",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("--metrics"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(diagnostic.contains("--write"), "diagnostic:\n{diagnostic}");
        assert!(diagnostic.contains("--probe"), "diagnostic:\n{diagnostic}");
    }

    /// `--metrics` conflicts with every other action outright, whether or not a `--write` is
    /// there for an explain to combine with.
    #[test]
    fn metrics_conflicts_with_every_non_write_action() {
        for args in [
            vec!["--show"],
            vec!["--explain"],
            vec!["--explain-analyze"],
            vec!["--explain", "--write", "out.vortex"],
            vec!["--explain-analyze", "--write", "out.vortex"],
        ] {
            let error = Cli::try_parse_from(
                [
                    "datafusion-sandbox",
                    "combine-refs",
                    "input",
                    "--metrics",
                    "runs",
                ]
                .into_iter()
                .chain(args.iter().copied()),
            )
            .err()
            .unwrap();
            let diagnostic = error.to_string();

            assert!(
                diagnostic.contains("cannot be used with"),
                "{args:?} diagnostic:\n{diagnostic}"
            );
            assert!(
                diagnostic.contains("--metrics"),
                "{args:?} diagnostic:\n{diagnostic}"
            );
        }
    }

    #[test]
    fn run_id_requires_metrics() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-refs",
            "input",
            "--write",
            "out.vortex",
            "--run-id",
            "run-1",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("--run-id"), "diagnostic:\n{diagnostic}");
        assert!(
            diagnostic.contains("--metrics"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn write_with_metrics_resolves_to_a_measured_write_with_a_generated_run_id() {
        let run = resolve(parse_combiner([
            "--write",
            "out.vortex",
            "--metrics",
            "runs",
            "--compression",
            "compact",
        ]))
        .unwrap();

        let Action::MeasuredWrite {
            write,
            metrics_directory,
            run_id,
        } = run.action
        else {
            panic!("expected a measured write, got {:?}", run.action);
        };
        assert_eq!(write.output_path, "out.vortex");
        assert_eq!(write.output_format.compression(), Some("compact"));
        assert_eq!(metrics_directory, MetricsDirectory::new("runs"));
        assert!(Uuid::parse_str(&run_id).is_ok(), "run id {run_id:?}");
        assert_eq!(run.row_limit, None);
    }

    #[test]
    fn an_explicit_run_id_is_honored() {
        let run = resolve(parse_combiner([
            "--write",
            "out.vortex",
            "--metrics",
            "runs",
            "--run-id",
            "sweep-3",
        ]))
        .unwrap();

        let Action::MeasuredWrite { run_id, .. } = run.action else {
            panic!("expected a measured write, got {:?}", run.action);
        };
        assert_eq!(run_id, "sweep-3");
    }

    #[test]
    fn allele_combiner_rejects_a_groups_argument() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-alleles",
            "input",
            "--groups",
            "2",
            "--show",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("--groups"), "diagnostic:\n{diagnostic}");
        assert!(
            diagnostic.contains("unexpected argument"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn groups_default_to_the_thread_count_under_grouped_merge() {
        let run = resolve(
            Cli::try_parse_from([
                "datafusion-sandbox",
                "--threads",
                "3",
                "combine-refs",
                "input",
                "--formulation",
                "grouped-merge",
                "--show",
            ])
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            run.formulation,
            Formulation::CombineRefsGroupedMerge {
                groups: NonZeroUsize::new(3).unwrap()
            }
        );
        assert_eq!(run.threads, NonZeroUsize::new(3).unwrap());
    }

    #[test]
    fn an_explicit_group_count_overrides_the_thread_count() {
        let run = resolve(parse_combiner([
            "--formulation",
            "grouped-merge",
            "--groups",
            "5",
            "--show",
        ]))
        .unwrap();

        assert_eq!(
            run.formulation,
            Formulation::CombineRefsGroupedMerge {
                groups: NonZeroUsize::new(5).unwrap()
            }
        );
    }

    #[test]
    fn groups_are_rejected_with_the_union_formulation() {
        let error = resolve(parse_combiner(["--groups", "2", "--show"]))
            .err()
            .unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("--groups"), "diagnostic:\n{diagnostic}");
        assert!(
            diagnostic.contains("grouped-merge"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn a_group_count_of_zero_is_rejected() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-refs",
            "input",
            "--formulation",
            "grouped-merge",
            "--groups",
            "0",
            "--show",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("--groups"), "diagnostic:\n{diagnostic}");
    }

    #[test]
    fn allele_combiner_rejects_a_formulation_argument() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-alleles",
            "input",
            "--formulation",
            "union",
            "--show",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("--formulation"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(
            diagnostic.contains("unexpected argument"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn reference_combiner_names_accepted_formulations_when_rejecting_a_removed_one() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-refs",
            "input",
            "--formulation",
            "one-scan",
            "--show",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("invalid value 'one-scan'"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(
            diagnostic.contains("possible values: union, grouped-merge, interval-merge"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn split_points_resolve_to_the_interval_merge_formulation() {
        let run = resolve(parse_combiner([
            "--formulation",
            "interval-merge",
            "--split-points",
            "22:1000,22:2000",
            "--write",
            "out",
        ]))
        .unwrap();

        assert_eq!(
            run.formulation,
            Formulation::CombineRefsIntervalMerge {
                split_points: "22:1000,22:2000".parse().unwrap()
            }
        );
        let Action::Write(target) = run.action else {
            panic!("expected a write");
        };
        assert_eq!(target.output_path, "out");
        assert_eq!(run.row_limit, None);
    }

    #[test]
    fn interval_merge_requires_split_points() {
        let error = resolve(parse_combiner([
            "--formulation",
            "interval-merge",
            "--write",
            "out",
        ]))
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("--split-points"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(
            diagnostic.contains("interval-merge"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn split_points_are_rejected_with_every_other_formulation() {
        for formulation in ["union", "grouped-merge"] {
            let error = resolve(parse_combiner([
                "--formulation",
                formulation,
                "--split-points",
                "22:1000",
                "--show",
            ]))
            .err()
            .unwrap();
            let diagnostic = error.to_string();

            assert!(
                diagnostic.contains("--split-points"),
                "{formulation} diagnostic:\n{diagnostic}"
            );
            assert!(
                diagnostic.contains("interval-merge"),
                "{formulation} diagnostic:\n{diagnostic}"
            );
        }
    }

    #[test]
    fn malformed_split_points_are_rejected_when_parsed() {
        for split_points in ["22:2000,22:1000", "chr22:1000", ""] {
            let error = Cli::try_parse_from([
                "datafusion-sandbox",
                "combine-refs",
                "input",
                "--formulation",
                "interval-merge",
                "--split-points",
                split_points,
                "--write",
                "out",
            ])
            .err()
            .unwrap();
            let diagnostic = error.to_string();

            assert!(
                diagnostic.contains("--split-points"),
                "{split_points:?} diagnostic:\n{diagnostic}"
            );
        }
    }

    /// `--limit` is accepted with every formulation, a file-per-interval one included, and under
    /// every action. No promise is made about the plan shape it leaves.
    #[test]
    fn a_limit_is_accepted_with_interval_merge() {
        for (action, action_arg) in [("--write", "out"), ("--show", "")] {
            let args = [
                "--formulation",
                "interval-merge",
                "--split-points",
                "22:1000",
                "--limit",
                "5",
                action,
                action_arg,
            ];
            let run = resolve(
                Cli::try_parse_from(
                    ["datafusion-sandbox", "combine-refs", "input"]
                        .into_iter()
                        .chain(args.into_iter().filter(|arg| !arg.is_empty())),
                )
                .unwrap(),
            )
            .unwrap();

            assert_eq!(run.row_limit, Some(5), "{action}");
        }
    }

    #[test]
    fn an_output_extension_is_rejected_with_interval_merge() {
        let error = resolve(parse_combiner([
            "--formulation",
            "interval-merge",
            "--split-points",
            "22:1000",
            "--write",
            "out.vortex",
        ]))
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("out.vortex"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(
            diagnostic.contains("interval-merge"),
            "diagnostic:\n{diagnostic}"
        );

        for path in ["out", "out/", "data/combined"] {
            let run = resolve(parse_combiner([
                "--formulation",
                "interval-merge",
                "--split-points",
                "22:1000",
                "--write",
                path,
            ]))
            .unwrap();
            let Action::Write(target) = run.action else {
                panic!("{path}: expected a write");
            };
            assert_eq!(target.output_path, path);
        }
    }

    #[test]
    fn output_extension_must_match_the_resolved_output_format() {
        let cli = parse_combiner(["--input-format", "parquet", "--write", "output.vortex"]);

        let error = resolve(cli).err().unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("output.vortex"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(diagnostic.contains("parquet"), "diagnostic:\n{diagnostic}");
    }

    #[test]
    fn output_format_must_accept_the_compression_value() {
        let cli = parse_combiner([
            "--output-format",
            "vortex",
            "--compression",
            "zstd(3)",
            "--write",
            "output.vortex",
        ]);

        let error = resolve(cli).err().unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("zstd(3)"), "diagnostic:\n{diagnostic}");
        assert!(diagnostic.contains("vortex"), "diagnostic:\n{diagnostic}");
    }

    /// The twenty-row default of `--show` is the row limit the CLI supplies when none is given.
    /// It does not depend on the formulation.
    #[test]
    fn show_resolves_to_collecting_twenty_rows() {
        let run = resolve(parse_combiner(["--show"])).unwrap();

        assert!(matches!(run.action, Action::Collect));
        assert_eq!(run.row_limit, Some(20));
    }

    #[test]
    fn an_explicit_show_limit_overrides_the_default() {
        let run = resolve(parse_combiner(["--show", "--limit", "7"])).unwrap();

        assert!(matches!(run.action, Action::Collect));
        assert_eq!(run.row_limit, Some(7));
    }

    #[test]
    fn comma_separated_sample_ids_resolve_to_a_sample_set() {
        let run = resolve(parse_combiner(["--samples", "HG00308,HG00309", "--show"])).unwrap();

        assert_eq!(
            run.sample_set,
            Some(vec!["HG00308".to_string(), "HG00309".to_string()])
        );
    }

    #[test]
    fn probe_requires_metrics() {
        let diagnostic = combiner_diagnostic(&["--probe"]);

        assert!(
            diagnostic.contains("--metrics"),
            "diagnostic:\n{diagnostic}"
        );
    }

    /// `--probe` conflicts outright with `--limit`, which would change the plan it measures,
    /// whether or not it writes, and with every other action but `--write`.
    #[test]
    fn probe_conflicts_with_limit_and_every_other_action() {
        for args in [
            vec!["--limit", "5"],
            vec!["--write", "out.vortex", "--limit", "5"],
            vec!["--show"],
            vec!["--explain"],
            vec!["--explain-analyze"],
        ] {
            let diagnostic = combiner_diagnostic(
                &["--probe", "--metrics", "runs"]
                    .into_iter()
                    .chain(args.iter().copied())
                    .collect::<Vec<_>>(),
            );

            assert!(
                diagnostic.contains("cannot be used with"),
                "{args:?} diagnostic:\n{diagnostic}"
            );
            assert!(
                diagnostic.contains("--probe"),
                "{args:?} diagnostic:\n{diagnostic}"
            );
        }
    }

    /// Every probe setting, as a flag and a valid value.
    const PROBE_SETTINGS: [[&str; 2]; 7] = [
        ["--poll-period", "0.05"],
        ["--batch", "2"],
        ["--precision", "0.01"],
        ["--consecutive", "2"],
        ["--window-groups", "8"],
        ["--min-duration", "10"],
        ["--max-duration", "5"],
    ];

    /// Every probe setting requires `--probe`, and is rejected even beside an argument `--probe`
    /// conflicts with, where clap would drop the requirement.
    #[test]
    fn every_probe_setting_requires_a_probe() {
        for setting in PROBE_SETTINGS {
            for args in [
                vec!["--write", "out.vortex"],
                vec!["--write", "out.vortex", "--limit", "5"],
                vec!["--show"],
                vec!["--explain"],
                vec!["--explain-analyze"],
            ] {
                let diagnostic = combiner_diagnostic(
                    &setting
                        .into_iter()
                        .chain(args.iter().copied())
                        .collect::<Vec<_>>(),
                );

                assert!(
                    diagnostic.contains(setting[0]),
                    "{setting:?} {args:?} diagnostic:\n{diagnostic}"
                );
            }
        }
    }

    #[test]
    fn probe_with_metrics_resolves_to_a_drained_probe_with_the_default_settings() {
        let run = resolve(parse_combiner(["--probe", "--metrics", "runs"])).unwrap();

        let Action::Probe {
            write,
            metrics_directory,
            run_id,
            settings,
        } = run.action
        else {
            panic!("expected a probe, got {:?}", run.action);
        };
        assert!(write.is_none(), "{write:?}");
        assert_eq!(metrics_directory, MetricsDirectory::new("runs"));
        assert!(Uuid::parse_str(&run_id).is_ok(), "run id {run_id:?}");
        assert_eq!(settings, ProbeSettings::default());
        assert_eq!(run.row_limit, None);
    }

    /// A probe with `--write` probes the write a plain write would perform, validated and
    /// compressed the same way.
    #[test]
    fn probe_with_a_write_resolves_to_a_written_probe_with_its_compression() {
        let run = resolve(parse_combiner([
            "--probe",
            "--write",
            "out.vortex",
            "--compression",
            "compact",
            "--metrics",
            "runs",
            "--max-duration",
            "5",
        ]))
        .unwrap();

        let Action::Probe {
            write: Some(write),
            settings,
            ..
        } = run.action
        else {
            panic!("expected a written probe, got {:?}", run.action);
        };
        assert_eq!(write.output_path, "out.vortex");
        assert_eq!(write.output_format.compression(), Some("compact"));
        assert_eq!(settings.max_duration, Duration::from_secs(5));

        let error = resolve(parse_combiner([
            "--probe",
            "--write",
            "out.parquet",
            "--metrics",
            "runs",
        ]))
        .err()
        .unwrap()
        .to_string();
        assert!(error.contains("out.parquet"), "{error}");
    }

    /// `--compression` still requires `--write` beside `--probe`, which may take one.
    #[test]
    fn compression_with_a_drained_probe_is_rejected() {
        let diagnostic =
            combiner_diagnostic(&["--probe", "--metrics", "runs", "--compression", "compact"]);

        assert!(
            diagnostic.contains("--compression"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(diagnostic.contains("--write"), "diagnostic:\n{diagnostic}");
    }

    #[test]
    fn a_probe_honors_its_run_id_and_settings_with_durations_in_decimal_seconds() {
        let run = resolve(parse_combiner(
            ["--probe", "--metrics", "runs", "--run-id", "sweep-4"]
                .into_iter()
                .chain(PROBE_SETTINGS.into_iter().flatten()),
        ))
        .unwrap();

        let Action::Probe {
            run_id, settings, ..
        } = run.action
        else {
            panic!("expected a probe, got {:?}", run.action);
        };
        assert_eq!(run_id, "sweep-4");
        assert_eq!(
            settings,
            ProbeSettings {
                poll_period: Duration::from_millis(50),
                batch_duration: Duration::from_secs(2),
                precision: 0.01,
                consecutive_checks: NonZeroU32::new(2).unwrap(),
                window_groups: 8,
                min_duration: Duration::from_secs(10),
                max_duration: Duration::from_secs(5),
            }
        );
    }

    #[test]
    fn a_duration_is_a_positive_finite_number_of_seconds() {
        assert_eq!(parse_seconds("300"), Ok(Duration::from_secs(300)));
        assert_eq!(parse_seconds("0.25"), Ok(Duration::from_millis(250)));
        for value in ["0", "-1", "inf", "NaN", "soon"] {
            let error = parse_seconds(value).unwrap_err();
            assert!(error.contains("positive"), "{value}: {error}");
        }
    }

    /// A precision target is a positive finite fraction, a probe needs at least one tight check,
    /// and an interval needs at least two groups.
    #[test]
    fn out_of_range_probe_settings_are_rejected() {
        for [flag, value] in [
            ["--precision", "0"],
            ["--precision", "-0.1"],
            ["--precision", "inf"],
            ["--consecutive", "0"],
            ["--window-groups", "1"],
            ["--window-groups", "1001"],
        ] {
            let diagnostic =
                combiner_diagnostic(&["--probe", "--metrics", "runs", &format!("{flag}={value}")]);

            assert!(
                diagnostic.contains(flag),
                "{flag} {value} diagnostic:\n{diagnostic}"
            );
        }
    }

    /// The diagnostic of a `combine-refs` command line with `args` that fails to parse.
    fn combiner_diagnostic(args: &[&str]) -> String {
        Cli::try_parse_from(
            ["datafusion-sandbox", "combine-refs", "input"]
                .into_iter()
                .chain(args.iter().copied()),
        )
        .err()
        .unwrap()
        .to_string()
    }

    fn parse_combiner<'a>(args: impl IntoIterator<Item = &'a str>) -> Cli {
        Cli::try_parse_from(
            [
                "datafusion-sandbox",
                "--threads",
                "1",
                "combine-refs",
                "input",
            ]
            .into_iter()
            .chain(args),
        )
        .unwrap()
    }

    #[test]
    fn rejects_a_thread_count_of_zero() {
        assert_eq!(parse_thread_count("1"), Ok(NonZeroUsize::MIN));
        assert_eq!(parse_thread_count("8"), Ok(NonZeroUsize::new(8).unwrap()));

        let err = parse_thread_count("0").unwrap_err();
        assert!(err.contains("at least 1"), "got: {err}");

        parse_thread_count("banana").expect_err("non-numeric thread counts must be rejected");
    }
}
