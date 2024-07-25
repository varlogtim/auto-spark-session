import os
import shutil

import papermill as pm
import determined as det


if __name__ == "__main__":
    info = det.get_cluster_info()

    with det.core.init(tensorboard_mode=det.core.TensorboardMode.MANUAL) as core_context:
        papermill_config = info.user_data.get("notebook_execution")

        pm.execute_notebook(
            input_path=papermill_config["in_path"],
            output_path=papermill_config["out_path"],
            parameters=papermill_config["parameters"],
            log_output=True,
            kernel_name="python3"
        )

        with core_context.checkpoint.store_path({"steps_completed": 0}) as (path, uuid):
            out_file = os.path.join(path, "result.ipynb")
            shutil.copyfile(papermill_config["out_path"], out_file)