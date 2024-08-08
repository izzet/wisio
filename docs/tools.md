# Tools

{{wisio}} is bundled with the following tools to help streamline the analysis process.

## Recorder to Parquet Converter

{{wisio}} provides an executable called `wisio-recorder2parquet` to convert raw Recorder traces into Parquet format. The executable is automatically installed during `pip install` and can be used as the following:

```bash
wisio-recorder2parquet /path/to/recorder/raw
```

The tool creates a folder called `_parquet` within the `/path/to/recorder/raw` folder and this new folder now can be passed to `wisio` as `analysis.trace_path=/path/to/recorder/raw/_parquet`.

The tool also supports MPI to convert large trace data into Parquet format in parallel.

```bash
mpirun -n 8 wisio-recorder2parquet /path/to/recorder/raw
```

This will create per-rank Parquet files in the `_parquet` folder. No further preprocessing is required as {{wisio}} is able to analyze multiple Parquet files thanks to its parallelizable execution flow.
