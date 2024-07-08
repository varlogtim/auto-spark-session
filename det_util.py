import os

from determined.common import api
from determined.common.api import bindings
from determined.experimental import client


def get_current_workspace() -> str:
    D = client.Determined()
    session = D._session

    def get_workspace_name_by_id(wid: int) -> str:
        resp = bindings.get_GetWorkspace(session, id=wid)
        return resp.workspace.name

    task_id = os.environ["DET_TASK_ID"]
    task_type = os.environ["DET_TASK_TYPE"]
    if task_type.lower() in [t.name for t in api.NTSC_Kind]:
        task_kind = getattr(api.NTSC_Kind, task_type.lower())
        deets = api.get_ntsc_details(session, task_kind, task_id)
        return get_workspace_name_by_id(deets.workspaceId)
    else:
        exp_id = os.environ["DET_EXPERIMENT_ID"]
        resp = bindings.get_GetExperiment(session, experimentId=int(exp_id))
        return resp.experiment.workspaceName
