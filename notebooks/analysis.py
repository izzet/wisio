import papermill as pm
from datetime import datetime


traces = [
    (
        "dftracer",
        "bert-v100-node-4-v1",
        "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/bert/v100/node-4/v1/COMPACT/*.pfw.gz",
        1e6,
    ),
    (
        "dftracer",
        "bert-v100-node-16-v3",
        "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/bert/v100/node-16/v3/COMPACT/*.pfw.gz",
        1e6,
    ),
    # (
    #     "dftracer",
    #     "cm1-node-1",
    #     "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/cm1/APP/node-1/v1/COMPACT/*.pfw.gz",
    #     1e6,
    # ),
    # (
    #     "dftracer",
    #     "cm1-node-32",
    #     "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/cm1/APP/node-32/v1/COMPACT/*.pfw.gz",
    #     1e6,
    # ),
    (
        "dftracer",
        "cosmoflow-dlio-v100-node-4-v1",
        "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/cosmoflow/dlio-v100/node-4/v1/COMPACT/*.pfw.gz",
        3e6,
    ),
    (
        "dftracer",
        "cosmoflow-dlio-v100-node-16-v3",
        "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/cosmoflow/dlio-v100/node-16/v3/COMPACT/*.pfw.gz",
        3e6,
    ),
    # (
    #     "dftracer",
    #     "montage-pegasus-dss-2deg-node-4",
    #     "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/montage/pegasus-dss-2deg/node-4/v1/COMPACT/*.pfw.gz",
    #     1e6,
    # ),
    # (
    #     "dftracer",
    #     "montage-pegasus-dss-2deg-node-16",
    #     "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/montage/pegasus-dss-2deg/node-16/v1/COMPACT/*.pfw.gz",
    #     1e6,
    # ),
    (
        "dftracer",
        "resnet50-dlio-v100-node-4-v1",
        "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/resnet50/dlio-v100/node-4/v1/RAW/*.pfw.gz",
        2e6,
    ),
    # (
    #     "dftracer",
    #     "resnet50-dlio-v100-node-16-v3",
    #     "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/resnet50/dlio-v100/node-16/v3/COMPACT/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "unet3d-dlio-v100-node-1-v1",
    #     "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/unet3d/dlio-v100/node-1/v1/RAW/*.pfw.gz",
    # ),
    (
        "dftracer",
        "unet3d-dlio-v100-node-16-v2",
        "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/unet3d/dlio-v100/node-16/v2/RAW/*.pfw.gz",
        4e6,
    ),
    # (
    #     "dftracer",
    #     "deepspeed-dlio-node-16",
    #     f"{dftracer_trace_dir.replace('traces-lfs', 'traces')}/megatron_deepspeed-1_16_8/megatron_deepspeed-1_16_8-COMPACT/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "deepspeed-dlio-scr-node-16",
    #     f"{dftracer_trace_dir.replace('traces-lfs', 'traces')}/megatron_deepspeed-1-100-1_16_8_scr/megatron_deepspeed-1-100-1_16_8_scr-COMPACT/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "1000-genome-pegasus-node-32",
    #     "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona/1000-genome/pegasus/node-32/v1/COMPACT/*.pfw.gz",
    # ),
]

bottleneck_dir = "/p/lustre3/iopp/wisio-bottlenecks"
checkpoint_dir = "/p/lustre3/iopp/wisio-checkpoints"
date = datetime.now().strftime("%Y%m%d%H%M")
view_types = ['proc_name', 'time_range']

for analyzer, trace_name, trace_path, time_granularity in traces:
    analysis_id = f"{analyzer}-{trace_name}-{'-'.join(view_types)}"
    pm.execute_notebook(
        "analysis.ipynb",
        f"analysis-{analysis_id}.ipynb",
        parameters=dict(
            analyzer=analyzer,
            app_view_types=view_types,
            bottleneck_dir=f"{bottleneck_dir}/{analysis_id}",
            checkpoint=True,
            checkpoint_dir=f"{checkpoint_dir}/{analysis_id}",
            cluster="external",
            cluster_restart_on_connect=True,
            cluster_scheduler_address="tcp://127.0.0.1:36047",
            dataloader_view_types=view_types,
            logical_view_types=False,
            output_max_bottlenecks=3,
            output_root_only=False,
            percentile=0.6,
            posix_view_types=view_types,
            run_dir=f".wisio/{analysis_id}",
            time_granularity=time_granularity,
            trace_path=trace_path,
        ),
    )

# bert (4 node - 8 GPUs per node = 32 processes)
# 1000 steps
# every step reads 48 images/samples of 2500 bytes each
# https://github.com/hariharan-devarajan/iopp/blob/system/corona/apps/dlio/script.sh
# do `prefetch_size` of 4GB
