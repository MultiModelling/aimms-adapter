import base64
import json
import subprocess
from time import sleep
from uuid import uuid4

from minio import S3Error

from tno.aimms_adapter.model.model import Model, ModelState
from tno.aimms_adapter.model.opera_accessdb.opera_access_importer import OperaAccessImporter, copy_clean_access_database
from tno.aimms_adapter.model.opera_accessdb.results_processor import OperaResultsProcessor
from tno.aimms_adapter.model.opera_esdl_parser.esdl_parser import OperaESDLParser
from tno.aimms_adapter.settings import EnvSettings
from tno.aimms_adapter.types import ModelRunInfo, OperaAdapterConfig, ModelRun
from tno.aimms_adapter import executor
from tno.shared.log import get_logger

logger = get_logger(__name__)


class Opera(Model):
    def request(self):

        model_run_id = str(uuid4())
        self.model_run_dict[model_run_id] = ModelRun(
            state=ModelState.ACCEPTED,
            config=None,
            result=None,
        )
        if len(self.model_run_dict.keys()) > 1:
            # there is already a model running
            self.model_run_dict[model_run_id].state = ModelState.PENDING
            return ModelRunInfo(
                state=ModelState.PENDING,
                reason="A model is already running",
                model_run_id = model_run_id
            )

        return ModelRunInfo(
            state=self.model_run_dict[model_run_id].state,
            model_run_id=model_run_id,
        )

    def start_aimms_model(self, config: OperaAdapterConfig, model_run_id):
        input_esdl: str
        if config.input_esdl_file_path[:7] == 'file://':
            logger.info(f"Loading ESDL from local disk at {config.input_esdl_file_path[7:]}")
            # local file
            with open(config.input_esdl_file_path[7:], 'r') as file:
                input_esdl = file.read()
        else: # assume minio
            try:
                logger.info(f"Loading ESDL from Inter Model Storage (Minio) at {config.input_esdl_file_path}")
                input_esdl_bytes = self.load_from_minio(config.input_esdl_file_path)
                if input_esdl_bytes is None:
                    logger.error(f"Error retrieving {config.input_esdl_file_path} from Minio")
                    return ModelRunInfo(
                        model_run_id=model_run_id,
                        state=ModelState.ERROR,
                        reason=f"Error retrieving {config.input_esdl_file_path} from Minio"
                    )
                else:
                    input_esdl = input_esdl_bytes.decode('utf-8')
            except S3Error as e:
                logger.error(f"Error retrieving {config.input_esdl_file_path} from Minio")
                return ModelRunInfo(
                    model_run_id=model_run_id,
                    state=ModelState.ERROR,
                    reason=f"Error retrieving {config.input_esdl_file_path} from Minio"
                )


        print('Input ESDL:', input_esdl)

        # convert ESDL to MySQL
        # logger.info("Converting ESDL using Universal Link")
        # ul = UniversalLink(host=EnvSettings.db_host(), database=EnvSettings.db_name(),
        #                    user=EnvSettings.db_user(), password=EnvSettings.db_password())
        # success, error = ul.esdl_to_db(input_esdl)
        parser = OperaESDLParser()
        try:
            esdl_in_dataframe, carriers = parser.parse(esdl_string=input_esdl)
        except Exception as e:
            logger.error(f"Parse exception for ESDL input: {e}")
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason=str(e)
            )

        copy_clean_access_database(EnvSettings.clean_access_database(), EnvSettings.access_database())
        logger.info("Importing ESDL into Opera database")
        oai = OperaAccessImporter()
        oai.start_import(esdl_data_frame=esdl_in_dataframe, carriers=carriers, access_database=EnvSettings.access_database())
        # start aimms via subprocess
        print(f"AIMMS binary at {EnvSettings.aimms_exe_path()}")
        print(f"AIMMS model at {EnvSettings.aimms_model_path()}")
        print(f"AIMMS start procedure {EnvSettings.aimms_procedure()}")

        aimms_exe_path = EnvSettings.aimms_exe_path()
        start_procedure = EnvSettings.aimms_procedure()
        aimms_model_path = EnvSettings.aimms_model_path()
        params = [aimms_exe_path, "-R", start_procedure, aimms_model_path] # --minimized

        # fake opera by running Ping command, that takes some time to run
        #params = ["ping", "-n", "10", "127.0.0.1"]

        logger.info("Starting AIMMS...")
        aimms = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        running = True
        output = []

        while running:
            for line in aimms.stdout: # this is blocking
                logger.info(f"AIMMS: {line.strip()}")
                output.append(line)
            running = aimms.poll() is None
            #if aimms.poll() is not None: # finished process
            #    running = False

        # wait for aimms to finish
        print()
        # get output
        if aimms.returncode == 0:
            logger.info("AIMMS has finished, collecting results...")
            esh = parser.get_energy_system_Hander()
            orp = OperaResultsProcessor(input_df=esdl_in_dataframe,
                                        esh=esh,
                                        output_path=EnvSettings.opera_output_folder())
            orp.update_production_capacities()
            updated_esdl_string = esh.to_string()

            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.SUCCEEDED,
                result = {'esdl': updated_esdl_string}
            )#, simulation_id
        else:
            # error
            logger.error(f'Running AIMMS failed, returncode={aimms.returncode}')
            logger.error(f'Output from AIMMS: {output}')
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason=f'AIMMS failed, return code: {aimms.returncode}. See logs for more info.',
            )




    # @staticmethod
    # def monitor_aimms_progress(simulation_id, model_run_id):
    # pass

    def threaded_run(self, model_run_id, config):
        print("Threaded_run:", config)

        # start AIMMS run
        start_aimms_info = self.start_aimms_model(config, model_run_id)
        if start_aimms_info.state == ModelState.RUNNING:
            # monitor AIMMS progress
            #monitor_essim_progress_info = Opera.monitor_essim_progress(simulation_id, model_run_id)
            #if monitor_essim_progress_info.state == ModelState.ERROR:
            #    return monitor_essim_progress_info
            return start_aimms_info
        else:
            return start_aimms_info

        ## Monitor KPI progress
        #monitor_kpi_progress_info = Opera.monitor_kpi_progress(simulation_id, model_run_id)
        #return monitor_kpi_progress_info

    def run(self, model_run_id: str):

        res = Model.run(self, model_run_id=model_run_id)

        if model_run_id in self.model_run_dict and self.model_run_dict[model_run_id].state == ModelState.RUNNING:
            config: OperaAdapterConfig = self.model_run_dict[model_run_id].config
            executor.submit_stored(model_run_id, self.threaded_run, model_run_id, config)
            res.state = self.model_run_dict[model_run_id].state
            return res
        else:
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason="Error in Opera.run(): model_run_id unknown or model is in wrong state"
            )

    def status(self, model_run_id: str):
        if model_run_id in self.model_run_dict:
            if not executor.futures.done(model_run_id):
                return ModelRunInfo(
                    state=self.model_run_dict[model_run_id].state,
                    model_run_id=model_run_id,
                    reason=f"executor.futures._state: {executor.futures._state(model_run_id)}"
                )
            else:
                #print("executor.futures._state: ", executor.futures._state(model_run_id))   # FINISHED
                future = executor.futures.pop(model_run_id)
                executor.futures.add(model_run_id, future)   # Put it back on again, so it can be retreived in results
                model_run_info = future.result()
                if model_run_info.result is not None:
                    print('Model execution result:', model_run_info.result)
                else:
                    logger.warning("No result in model_run_info variable")
                return model_run_info
        else:
            return ModelRunInfo(
                model_run_id=model_run_id,
                state=ModelState.ERROR,
                reason="Error in ESSIM.status(): model_run_id unknown"
            )

    def process_results(self, result):
        return result['esdl']  # returns the ESDL string of the updated ESDL

    def results(self, model_run_id: str):
        # Issue: if status already runs executor.future.pop, future does not exist anymore
        if executor.futures.done(model_run_id):
            if model_run_id in self.model_run_dict:
                future = executor.futures.pop(model_run_id)
                model_run_info = future.result()
                if model_run_info.result is not None:
                    print("Opera Run has ESDL result:")
                    print(model_run_info.result)
                else:
                    logger.warning("No result in model_run_info variable")

                self.model_run_dict[model_run_id].state = model_run_info.state
                if model_run_info.state == ModelState.SUCCEEDED:
                    self.model_run_dict[model_run_id].result = model_run_info.result

                    Model.store_result(self, model_run_id=model_run_id, result=model_run_info.result)
                else:
                    self.model_run_dict[model_run_id].result = {}

                return Model.results(self, model_run_id=model_run_id)
            else:
                return ModelRunInfo(
                    model_run_id=model_run_id,
                    state=ModelState.ERROR,
                    reason="Error in ESSIM.results(): model_run_id unknown"
                )
        else:
            return Model.results(self, model_run_id=model_run_id)
