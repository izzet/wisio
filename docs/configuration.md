# Configuration

{{wisio}} is powered by [Hydra](https://hydra.cc/) and therefore its configuration is very flexible.

```yaml
analysis:
  bottleneck_dir: /path/to/wisio/bottlenecks
  trace_path: /path/to/trace
  type: DARSHAN
  exclude_bottlenecks: []
  exclude_characteristics: []
  metrics:
    - iops
  logical_view_types: false
  threshold: 45
  time_granularity: 1000000.0
  view_types:
    - file_name
    - proc_name
    - time_range
checkpoint:
  dir: /path/to/wisio/checkpoints
  enabled: false
cluster:
  type: LOCAL
  dashboard_port: 0
  debug: false
  host: ""
  local_dir: /path/to/worker_dir
  memory: 0
  n_threads_per_worker: 16
  n_workers: 8
  processes: false
output:
  type: CONSOLE
  compact: true
  group_behavior: false
  max_bottlenecks: 2
  name: ""
  root_only: true
  show_debug: false
  show_characteristics: true
  show_header: true
  view_names: []
debug: false
verbose: false
```
