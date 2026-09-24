// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker warning.
#![cfg_attr(
    all(target_os = "macos", debug_assertions),
    allow(
        linker_messages,
        reason = "Apple ld falls back to DWARF when the CLI exceeds compact unwind's 16 MiB offset range; rust-lang/rust#159105 tracks this diagnostic"
    )
)]

use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion::error::{DataFusionError, Result};

use datafusion_sandbox::combiner_run::{Action, CombinerRun};
use datafusion_sandbox::format::{InputFormat, OutputFormat};
use datafusion_sandbox::formulation::Formulation;
use datafusion_sandbox::locus::SplitPoints;
use datafusion_sandbox::metrics_directory::MetricsDirectory;
use datafusion_sandbox::ordered_frame::OutputLayout;
use datafusion_sandbox::write::WriteTarget;
use datafusion_sandbox::{pipeline, split_points};
use std::{num::NonZeroUsize, path::Path};
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
    /// DIR/metrics/<ID>.parquet, both Parquet whatever the output format. DIR may be any path
    /// --write accepts. Requires --write and performs it.
    #[arg(
        long,
        value_name = "DIR",
        requires = "write",
        conflicts_with_all = ["show", "explain", "explain_analyze"]
    )]
    metrics: Option<String>,
    /// The id naming this run in the metrics tables. Defaults to a generated UUID.
    #[arg(long, value_name = "ID", requires = "metrics")]
    run_id: Option<String>,
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

/// The action flags. At least one is required. `--explain` and `--explain-analyze` may combine
/// with `--write`, in which case they render or analyze the write's plan; every other pair
/// conflicts.
#[derive(Args)]
#[group(required = true, multiple = true)]
struct ActionArgs {
    /// Write the combined rows to PATH. With --explain or --explain-analyze, the plan shown is
    /// the write's, and --explain-analyze performs the write.
    #[arg(long, value_name = "PATH")]
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
}

enum CliAction {
    Write(String),
    Show,
    Explain { write: Option<String> },
    ExplainAnalyze { write: Option<String> },
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
        } = args;
        match (write, show, explain, explain_analyze) {
            (Some(path), false, false, false) => Ok(Self::Write(path)),
            (None, true, false, false) => Ok(Self::Show),
            (write, false, true, false) => Ok(Self::Explain { write }),
            (write, false, false, true) => Ok(Self::ExplainAnalyze { write }),
            _ => Err(DataFusionError::Configuration(
                "an action is required: --write, --show, --explain, or --explain-analyze, where \
                 --explain and --explain-analyze may combine with --write"
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
            let path_is_directory = args.path.ends_with('/') || Path::new(&args.path).is_dir();
            validate_input_path(&args.path, &input_format, path_is_directory)?;
            let points =
                split_points::row_balanced(args.path, input_format, args.intervals, threads)?;
            println!("{points}");
        }
        command => {
            let run = resolve(Cli { command, threads })?;
            println!("formulation: {}", run.formulation);
            if let Action::MeasuredWrite { run_id, .. } = &run.action {
                println!("run id: {run_id}");
            }
            let outcome = run.execute()?;
            println!("{}", outcome.render()?);
        }
    }
    Ok(())
}

/// Rejects a file path whose recognized extension contradicts the selected input format.
fn validate_input_path(
    input_path: &str,
    input_format: &InputFormat,
    path_is_directory: bool,
) -> Result<()> {
    if path_is_directory {
        return Ok(());
    }
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
        let error = validate_input_path("combined.parquet", &InputFormat::VORTEX, false)
            .expect_err("a Parquet extension must contradict the Vortex input format");

        assert!(
            error.to_string().contains(
                "input path 'combined.parquet' has extension '.parquet', which contradicts input format 'vortex'"
            ),
            "{error}"
        );
        validate_input_path("combined.parquet", &InputFormat::VORTEX, true)
            .expect("a directory path is not format inference");
    }

    #[test]
    fn an_action_is_required() {
        let error = Cli::try_parse_from(["datafusion-sandbox", "combine-refs", "input"])
            .err()
            .unwrap();
        let diagnostic = error.to_string();

        for action in ["--write", "--show", "--explain", "--explain-analyze"] {
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
    fn metrics_requires_a_write_action() {
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

    fn parse_combiner<const N: usize>(args: [&str; N]) -> Cli {
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
