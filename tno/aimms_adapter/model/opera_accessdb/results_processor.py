import os

import pandas as pd
from esdl import esdl
from esdl.esdl_handler import EnergySystemHandler

from tno.aimms_adapter.model.opera_esdl_parser.unit import convert_to_unit, POWER_IN_GW, POWER_IN_W
from tno.shared.log import get_logger

log = get_logger(__name__)


class OperaResultsProcessor:
    output_path: str
    esh: EnergySystemHandler
    df: pd.DataFrame

    def __init__(self, output_path: str, esh: EnergySystemHandler, input_df: pd.DataFrame):
        self.output_path = output_path
        self.esh = esh
        self.df = input_df

        es = self.esh.get_energy_system()
        es.description = es.description + "\nIncluding Opera results"
        es.version = str(float(es.version) + 1.0)

    def get_updated_energysystem(self):
        return self.esh.get_energy_system()

    def update_production_capacities(self):
        capacity = pd.read_csv(self.output_path + os.sep + "Capacity.csv", encoding='latin_1')
        # Regions,Option,Variant,Construction year,View year,Capacity
        capacity.groupby(['Option'], axis=1)
        # split option into nr and name
        capacity[['Nr', 'Name']] = capacity['Option'].str.split(' ', n=1, expand=True)
        for index, row in self.df.iterrows():  # iterate through input list
            asset_name  = row['name']
            found = capacity[capacity['Name'] == asset_name]
            if not found.empty:
                updated_capacity_in_GW = found['Capacity'].item()
                min_capacity_range = row['power_min'] # convert_to_unit(row['power_min'], POWER_IN_W, POWER_IN_GW)
                max_capacity_range = row['power_max'] #convert_to_unit(row['power_max'], POWER_IN_W, POWER_IN_GW)
                print(f"Found updated capacity for {asset_name}: {updated_capacity_in_GW} GW in range [{min_capacity_range:.2f}-{max_capacity_range:.2f}]")
                id = row['id']
                asset = self.esh.get_by_id(id)
                if asset and hasattr(asset, 'power'):
                    power_in_w = convert_to_unit(updated_capacity_in_GW, POWER_IN_GW, POWER_IN_W)
                    asset.power = power_in_w
                else:
                    log.error(f"Can't find asset in ESDL: asset_id={id}, asset_name={asset_name}")
