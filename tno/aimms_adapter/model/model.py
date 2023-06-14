from abc import ABC, abstractmethod
from io import BytesIO
from typing import Dict
from uuid import uuid4

from minio import Minio

from tno.aimms_adapter.settings import EnvSettings
from tno.aimms_adapter.types import ModelRun, ModelState, ModelRunInfo
from tno.shared.log import get_logger

logger = get_logger(__name__)


class Model(ABC):
    def __init__(self):
        self.model_run_dict: Dict[str, ModelRun] = {}

        self.minio_client = None
        if EnvSettings.minio_endpoint():
            logger.info(f"Connecting to Minio Object Store at {EnvSettings.minio_endpoint()}")
            self.minio_client = Minio(
                endpoint=EnvSettings.minio_endpoint(),
                secure=EnvSettings.minio_secure(),
                access_key=EnvSettings.minio_access_key(),
                secret_key=EnvSettings.minio_secret_key()
            )
            buckets = self.minio_client.list_buckets()

            for bucket in buckets:
                logger.info(f" - Bucket: {bucket.name}, created {bucket.creation_date}")

        else:
            logger.info("No Minio Object Store configured")

    def request(self):
        model_run_id = str(uuid4())
        self.model_run_dict[model_run_id] = ModelRun(
            state=ModelState.ACCEPTED,
            config=None,
            result=None,
        )

        return ModelRunInfo(
            state=self.model_run_dict[model_run_id].state,
            model_run_id=model_run_id,
        )

    def initialize(self, model_run_id: str, config=None):
        if model_run_id in self.model_run_dict:
            self.model_run_dict[model_run_id].config = config
            self.model_run_dict[model_run_id].state = ModelState.READY
            return ModelRunInfo(
                state=self.model_run_dict[model_run_id].state,
                model_run_id=model_run_id,
            )
        else:
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason="Error in Model.initialize(): model_run_id unknown"
            )

    def load_from_minio(self, path):
        bucket = path.split("/")[0]
        rest_of_path = "/".join(path.split("/")[1:])

        response = self.minio_client.get_object(bucket, rest_of_path)
        if response:
            logger.info(f"Minio response: {response}")
            return response.data
        else:
            logger.error(f"Failed to retrieve from Minio: bucket={bucket}, path={rest_of_path}")
            return None

    @abstractmethod
    def process_results(self, result):
        pass

    def store_result(self, model_run_id: str, result):
        if model_run_id in self.model_run_dict:
            res = self.process_results(result)
            if res:
                path = self.model_run_dict[model_run_id].config.output_esdl_file_path
                if self.minio_client:
                    content = BytesIO(bytes(res, 'utf8'))

                    bucket = path.split("/")[0]
                    rest_of_path = "/".join(path.split("/")[1:])

                    if not self.minio_client.bucket_exists(bucket):
                        self.minio_client.make_bucket(bucket)

                    self.minio_client.put_object(bucket, rest_of_path, content, content.getbuffer().nbytes)
                else:
                    if path[:7] == 'file://': # local file
                        filename = path[7:]
                        logger.info("Writing result ESDL to disk: " + filename)
                        with open(filename, 'w') as file:
                            file.write(res)
                    else:
                        raise IOError("Don't know how to write file " + path)

                self.model_run_dict[model_run_id].result = {
                    "path": path
                }
            else:
                self.model_run_dict[model_run_id].result = {
                    "result": res
                }
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.SUCCEEDED,
            )
        else:
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason="Error in Model.store_result(): model_run_id unknown"
            )

    def run(self, model_run_id: str):
        if model_run_id in self.model_run_dict:
            state = self.model_run_dict[model_run_id].state
            if state != ModelState.READY:
                res = ModelRunInfo(
                    state=self.model_run_dict[model_run_id].state,
                    model_run_id=model_run_id,
                    reason="Error: Model is not in READY state"
                )
                return res

            self.model_run_dict[model_run_id].state = ModelState.RUNNING
            return ModelRunInfo(
                state=self.model_run_dict[model_run_id].state,
                model_run_id=model_run_id,
            )
        else:
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason="Error in Model.run(): model_run_id unknown"
            )

    def status(self, model_run_id: str):
        if model_run_id in self.model_run_dict:
            # Dummy behaviour: Query status once, to let finish model
            self.model_run_dict[model_run_id].state = ModelState.SUCCEEDED

            return ModelRunInfo(
                state=self.model_run_dict[model_run_id].state,
                model_run_id=model_run_id,
            )
        else:
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason="Error in Model.status(): model_run_id unknown"
            )

    def results(self, model_run_id: str):
        if model_run_id in self.model_run_dict:
            return ModelRunInfo(
                state=self.model_run_dict[model_run_id].state,
                model_run_id=model_run_id,
                result=self.model_run_dict[model_run_id].result,
            )
        else:
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason="Error in Model.results(): model_run_id unknown"
            )

    def remove(self, model_run_id: str):
        if model_run_id in self.model_run_dict:
            del self.model_run_dict[model_run_id]
            # check if models are in pending state and accept the first one we find.
            if len(self.model_run_dict.keys()) > 0:
                for m in self.model_run_dict.values():
                    if m.state == ModelState.PENDING:
                        m.state == ModelState.ACCEPTED
                        break


            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.UNKNOWN,
            )
        else:
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason="Error in Model.remove(): model_run_id unknown"
            )