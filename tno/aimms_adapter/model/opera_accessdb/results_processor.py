import os
from os.path import abspath

import esdl
import numpy as np
import pandas as pd
from esdl.esdl_handler import EnergySystemHandler

from tno.aimms_adapter.model.opera_esdl_parser.unit import convert_to_unit, POWER_IN_GW, POWER_IN_W
from tno.shared.log import get_logger

log = get_logger(__name__)


class OperaResultsProcessor:
    output_path: str
    esh: EnergySystemHandler
    df: pd.DataFrame

    def __init__(self, output_path: str, esh: EnergySystemHandler, input_df: pd.DataFrame):
        """esh: the EnergySystemHandler that is used by the OperaESDLParser, i.e. with loaded input EnergySystem"""
        self.output_path = output_path
        self.esh = esh
        self.df = input_df

        es = self.esh.get_energy_system()
        es.description = es.description + "\nIncluding Opera results"
        es.version = str(float(es.version) + 1.0)
        log.debug(f"Expecting Opera outputs in {abspath(output_path)}")

    def get_updated_energysystem(self):
        return self.esh.get_energy_system()

    def update_production_capacities(self):
        """
        Updates the production capacities of all assets in the input dataframe using the id and name of the option
        and updates this in the energy system that is loaded with the esh specified in the constructor of this class.
        """
        capacity = pd.read_csv(self.output_path + os.sep + "Capacity.csv", encoding='latin_1')
        unitOfCapacity = pd.read_csv(self.output_path + os.sep + "UoCapacity.csv", encoding='latin_1')
        capacity = pd.merge(left=capacity, right=unitOfCapacity, on='Option', )
        # Regions,Option,Variant,Construction year,View year,Capacity
        #capacity = capacity.groupby(['Option', 'UoCapacity'], axis=1).sum()
        # split option into nr and name
        capacity[['Nr', 'Name']] = capacity['Option'].str.split(' ', n=1, expand=True)
        for index, row in self.df.iterrows():  # iterate through input list
            asset_name = row['name']
            found = capacity[capacity['Name'] == asset_name]
            if not found.empty:
                updated_capacity_in_GW = found['Capacity'].item()
                unit = found['UoCapacity'].item()
                source_unit = POWER_IN_GW
                if unit != 'GW':
                    log.info(f"Ignoring {asset_name} as its unit is not GW, which is incompatible with the power attribute of this ESDL asset")
                    continue  # we can only handle GW for power attribute now
                min_capacity_range = row['power_min'] # convert_to_unit(row['power_min'], POWER_IN_W, POWER_IN_GW)
                max_capacity_range = row['power_max'] #convert_to_unit(row['power_max'], POWER_IN_W, POWER_IN_GW)

                id = row['id']
                asset: esdl.EnergyAsset = self.esh.get_by_id(id)
                if asset and hasattr(asset, 'power'):
                    power_in_w = convert_to_unit(updated_capacity_in_GW, source_unit, POWER_IN_W)
                    old_power_in_GW = convert_to_unit(asset.power, POWER_IN_W, source_unit)
                    if min_capacity_range and max_capacity_range and \
                            not np.isnan(min_capacity_range) and not np.isnan(max_capacity_range):
                        log.debug(f"Found updated capacity for {asset_name}: {updated_capacity_in_GW} GW in range [{min_capacity_range:.2f}-{max_capacity_range:.2f}], old power={old_power_in_GW} GW")
                        # clear constraints, as these have been solved
                        asset.constraint.clear()
                    else:
                        log.debug(f"Found updated capacity for {asset_name}: {updated_capacity_in_GW} GW (old power={old_power_in_GW} GW)")
                    asset.power = power_in_w

                else:
                    log.error(f"Can't find asset in ESDL: asset_id={id}, asset_name={asset_name} with attribute 'power'")
