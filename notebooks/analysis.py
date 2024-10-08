import papermill as pm

checkpoint_dir = "/usr/workspace/iopp/wisio_logs/_checkpoints"
dftracer_trace_dir = "/p/lustre3/iopp/dftracer-traces-lfs/v1.0.5-develop/corona"

traces = [
    # (
    #     "dftracer",
    #     "bert-v100-node-4-v1",
    #     f"{dftracer_trace_dir}/bert/v100/node-4/v1/RAW/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "bert-v100-node-16-v2",
    #     f"{dftracer_trace_dir}/bert/v100/node-16/v2/RAW/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "cm1-n1-ppn48",
    #     f"{dftracer_trace_dir}/cm1_1_48_20240926/cm1_1_48_20240926-COMPACT/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "cm1-n32-ppn32",
    #     f"{dftracer_trace_dir}/cm1_32_32_20240926/cm1_32_32_20240926-COMPACT/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "cosmoflow-dlio-v100-node-4-v1",
    #     f"{dftracer_trace_dir}/cosmoflow/dlio-v100/node-4/v1/RAW/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "resnet50-dlio-v100-node-4-v1",
    #     f"{dftracer_trace_dir}/resnet50/dlio-v100/node-4/v1/RAW/*.pfw.gz",
    # ),
    # (
    #     "dftracer",
    #     "resnet50-dlio-v100-node-16-v2",
    #     f"{dftracer_trace_dir}/resnet50/dlio-v100/node-16/v2/RAW/*.pfw.gz",
    # ),
    (
        "dftracer",
        "deepspeed-dlio-node-16",
        f"{dftracer_trace_dir.replace('traces-lfs', 'traces')}/megatron_deepspeed-1_16_8/megatron_deepspeed-1_16_8-COMPACT/*.pfw.gz",
    ),
    (
        "dftracer",
        "deepspeed-dlio-scr-node-16",
        f"{dftracer_trace_dir.replace('traces-lfs', 'traces')}/megatron_deepspeed-1-100-1_16_8_scr/megatron_deepspeed-1-100-1_16_8_scr-COMPACT/*.pfw.gz",
    ),
]

for analyzer, trace_name, trace_path in traces:
    pm.execute_notebook(
        "analysis.ipynb",
        f"analysis-{analyzer}-{trace_name}.ipynb",
        parameters=dict(
            analyzer=analyzer,
            checkpoint=True,
            checkpoint_dir=f"{checkpoint_dir}/{analyzer}-{trace_name}",
            logical_view_types=False,
            output_max_bottlenecks=1,
            output_root_only=False,
            run_dir=f".wisio/{analyzer}/{trace_name}",
            trace_path=trace_path,
        ),
    )
