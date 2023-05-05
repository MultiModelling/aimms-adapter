from typing import Tuple, Union, Optional, List

from esdl.esdl_handler import EnergySystemHandler
from .unit import convert_to_unit, POWER_IN_GW, ENERGY_IN_PJ, COST_IN_MEur, POWER_IN_W, COST_IN_Eur_per_MWh
import esdl
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

# current asset types that are not supported by this parser
ignore_asset_tuple = (esdl.Transport, esdl.Import, esdl.Storage, esdl.Export)

class OperaESDLParser:

    def __init__(self):
        self.esh = EnergySystemHandler()

    def parse(self, esdl_string: str):
        """
        Extracts Cost, ranges of production and values of demand
        :param esdl_string:
        :return:
        """
        print(f"Power unit : {POWER_IN_GW.description}")
        print(f"Energy unit: {ENERGY_IN_PJ.description}")
        print(f"Cost unit: {COST_IN_MEur.description}")
        print(f"Marginal Cost unit: {COST_IN_Eur_per_MWh.description}")

        self.esh.load_from_string(esdl_string)
        energy_assets = self.esh.get_all_instances_of_type(esdl.EnergyAsset)
        df = pd.DataFrame({'category': pd.Series(dtype='str'),
                           'esdlType': pd.Series(dtype='str'),
                           'name': pd.Series(dtype='str'),
                           'power_min': pd.Series(dtype='float'),
                           'power_max': pd.Series(dtype='float'),
                           'power': pd.Series(dtype='float'),
                           'efficiency': pd.Series(dtype='float'),
                           'investment_cost': pd.Series(dtype='float'),
                           'o_m_cost': pd.Series(dtype='float'),
                           'marginal_cost': pd.Series(dtype='float'),
                           'carrier_in': pd.Series(dtype='str'),
                           'carrier_out': pd.Series(dtype='str'),
                           'profiles_in': pd.Series(dtype='str'),
                           'profiles_out': pd.Series(dtype='str'),
                           'opera_equivalent': pd.Series(dtype='str')
                           })
        for asset in energy_assets:
            max_power = None
            if not isinstance(asset, ignore_asset_tuple):
                asset: esdl.EnergyAsset = asset
                print(f'Converting {asset.name}')
                category = esdl_category(asset)
                power_range, unit = extract_range(asset, 'power')
                print("Power range: ", power_range)
                if power_range:
                    power_range = tuple([convert_to_unit(v, unit, POWER_IN_GW) for v in power_range])
                if hasattr(asset, 'power'):
                    max_power = convert_to_unit(asset.power, POWER_IN_W, POWER_IN_GW) if asset.power else None
                efficiency = extract_efficiency(asset)
                costs = extract_costs(asset)
                carrier_in_list, carrier_out_list = extract_carriers(asset)
                carrier_in = ", ".join(carrier_in_list)
                carrier_out = ", ".join(carrier_out_list)
                singlevalue_profiles_in, singlevalue_profiles_out = extract_port_singlevalue_profiles(asset, ENERGY_IN_PJ)
                profiles_in = ", ".join([str(p) for p in singlevalue_profiles_in])
                profiles_out = ", ".join([str(p) for p in singlevalue_profiles_out])
                print(f"profiles: {singlevalue_profiles_in} and out {singlevalue_profiles_out}")
                opera_equivalent = find_opera_equivalent(asset)
                print(f'{asset.eClass.name}, {asset.name}, power_range={power_range}, power={max_power}, costs={costs}' )
                s = [category, asset.eClass.name, asset.name,
                     power_range[0] if power_range else None, power_range[1] if power_range else None,
                     max_power, efficiency, costs[0], costs[1], costs[2],
                     carrier_in, carrier_out, profiles_in, profiles_out, opera_equivalent]
                df.loc[len(df)] = s

        print(df)
        df.to_csv('output.csv')

class ParseException(Exception):
    pass


def extract_range(asset: esdl.EnergyAsset, attribute_name:str) -> Tuple[Tuple[float, float], esdl.QuantityAndUnitType]:
    """
    Returns the Range constraint of this energy asset as a tuple, plus the unit of the range
    Returns None, None if nothing is found
    :param asset:
    :return:
    """
    constraints = asset.constraint
    for c in constraints:
        if isinstance(c, esdl.RangedConstraint):
            rc: esdl.RangedConstraint = c
            if rc.attributeReference.lower() == attribute_name.lower():
                constraint_range: esdl.Range = rc.range
                if constraint_range.profileQuantityAndUnit is None:
                    print(f"No unit specified for constraint of asset {asset.name}, assuming WATT")
                    constraint_range.profileQuantityAndUnit = POWER_IN_W
                return (constraint_range.minValue, constraint_range.maxValue), constraint_range.profileQuantityAndUnit
            else:
                raise ParseException(f'Can\'t find an Ranged constrained for asset {asset.name} with attribute name {attribute_name}')
    return None, None # make sure unpacking works


def extract_singlevalue(profile: esdl.GenericProfile) -> Optional[float]:
    """
    Returns the value of a SingleValue profile or 0 if not found.
    :param profile:
    :return:
    """
    if profile is not None and isinstance(profile, esdl.SingleValue):
        single_value: esdl.SingleValue = profile
        # check for units here!
        # single_value.profileQuantityAndUnit
        return single_value.value
    print(f"Cannot convert profile {profile.name} of {profile.eContainer()} to a SingleValue")
    return None


def extract_efficiency(asset: esdl.EnergyAsset) -> float:
    # todo: Storage has charge & discharge efficiencies
    # Conversion: AbstractBasicConversion has efficiency
    # HeatPump has COP...
    if hasattr(asset, 'efficiency'):
        efficiency = asset.efficiency
        return efficiency
    else:
        return 1.0

def extract_costs(asset: esdl.EnergyAsset):
    o_m_cost_normalized = None
    investment_costs_normalized = None
    marginal_cost_normalized = None
    costinfo: esdl.CostInformation = asset.costInformation
    if costinfo:
        o_m_costs_profile:esdl.GenericProfile = costinfo.fixedOperationalAndMaintenanceCosts
        if o_m_costs_profile:
            o_m_costs = extract_singlevalue(o_m_costs_profile)
            o_m_cost_normalized = convert_to_unit(o_m_costs, o_m_costs_profile.profileQuantityAndUnit, COST_IN_MEur)
        investment_costs_profile: esdl.GenericProfile = costinfo.investmentCosts
        if investment_costs_profile:
            investment_costs = extract_singlevalue(investment_costs_profile)
            investment_costs_normalized = convert_to_unit(investment_costs, investment_costs_profile.profileQuantityAndUnit, COST_IN_MEur)
        marginal_cost_profile: esdl.GenericProfile = costinfo.marginalCosts
        if marginal_cost_profile:
            marginal_cost = extract_singlevalue(marginal_cost_profile)
            marginal_cost_normalized = convert_to_unit(marginal_cost, marginal_cost_profile.profileQuantityAndUnit, COST_IN_Eur_per_MWh)
    return o_m_cost_normalized, investment_costs_normalized, marginal_cost_normalized


def extract_carriers(asset: esdl.EnergyAsset) -> Tuple[List[str], List[str]]:
    ports = asset.port
    carrier_in_list = []
    carrier_out_list = []
    for p in ports:
        p: esdl.Port = p
        if p.carrier:
            if isinstance(p, esdl.InPort):
                carrier_in_list.append(p.carrier.name)
            else:
                carrier_out_list.append(p.carrier.name)

    return carrier_in_list, carrier_out_list

def extract_port_singlevalue_profiles(asset: esdl.EnergyAsset, target_unit:esdl.QuantityAndUnitType) -> Tuple[List[str], List[str]]:
    ports = asset.port
    singlevalue_in_list = []
    singlevalue_out_list = []
    for p in ports:
        p: esdl.Port = p
        if p.profile and len(p.profile) > 0:
            profile: esdl.GenericProfile = p.profile[0]  # TODO: only uses first profile!
            if isinstance(profile, esdl.SingleValue):
                if isinstance(p, esdl.InPort):
                    singlevalue_in_list.append(convert_to_unit(extract_singlevalue(profile), profile.profileQuantityAndUnit, target_unit))
                else:
                    singlevalue_out_list.append(convert_to_unit(extract_singlevalue(profile), profile.profileQuantityAndUnit, target_unit))

    return singlevalue_in_list, singlevalue_out_list



def find_opera_equivalent(asset: esdl.EnergyAsset) -> str | None:
    if isinstance(asset, esdl.Electrolyzer):
        return "H2 Large-scale electrolyser"
    elif isinstance(asset, esdl.MobilityDemand):
        # todo check for carrier
        md: esdl.MobilityDemand = asset
        if md.fuelType == esdl.MobilityFuelTypeEnum.HYDROGEN:
            if esdl.VehicleTypeEnum.CAR in md.type:
                return " H2 auto"  # mind the space
            elif esdl.VehicleTypeEnum.VAN in md.type:
                return "H2 van"  # mind the space
            elif esdl.VehicleTypeEnum.TRUCK in md.type:
                return "H2 truck with energy consumption reduction"
        else:
            return "REF Finale vraag verkeer th" # not sure if this is ok, as it is Final demand...
    elif isinstance(asset, esdl.GasConversion):
        gconv: esdl.GasConversion = asset
        if gconv.type == esdl.GasConversionTypeEnum.SMR:
            return "H2 uit SMR met CCS plus"
        elif gconv.type == esdl.GasConversionTypeEnum.ATR:
            print(f"Cannot map {asset.name} of type ATR to an Opera equivalent")
            return None
        else:
            return "H2 uit SMR met CCS plus"
    elif isinstance(asset, esdl.WindTurbine) or isinstance(asset, esdl.WindPark):
        # WindPark is a subtype of WindTurbine
        windturbine: esdl.WindTurbine = asset
        if windturbine.type == esdl.WindTurbineTypeEnum.WIND_ON_LAND:
            return "Wind op Land band 1"
        elif windturbine.type == esdl.WindTurbineTypeEnum.WIND_AT_SEA:
            return "Wind op Zee band 1"
        else:
            print(f"Unmapped type {windturbine.type} for {asset.name}, mapping to Wind op Zee for Opera equivalent")
            return "Wind op Zee band 1"
    elif isinstance(asset, esdl.PVPanel): # superclass of PVPark and PVInstallation
        # todo handle sector information here to map to right sector PV production
        panel: esdl.PVPanel = asset
        return "Solar-PV Residential" # or "Solar -PV industry" # mind the space!
    elif isinstance(asset, esdl.Import):
        carrier:str = None
        for port in asset.port:
            if isinstance(port, esdl.OutPort):
                carrier = port.carrier.name if port.carrier else None
        if carrier:
            if carrier.lower().startswith("elec"):
                # electricity import
                return "REF E import Flexnet"
            elif carrier.lower().startswith("h2") or  carrier.lower().startswith("waterstof") or  carrier.lower().startswith("hydrogen"):
                return "Import H2 to H2 domestic"
            elif carrier.lower().startswith("aardgas") or  carrier.lower().startswith("natural gas"):
                return "REF Gaswinning en -import"
            else:
                return None
    elif isinstance(asset, esdl.Export):
        # TODO: adapt to carriers as with import
        return "H2 domestic to export"
    else:
        print(f"Cannot map {asset.name} to an Opera equivalent")
        return None


'''
$ SELECT DISTINCT(Energiedrager) FROM [Opties]

                 Energiedrager
0                         None
1                      Aardgas
2                      Benzine
3              Biobrandstoffen
4           Biobrandstoffen FT
5                  Bio-ethanol
6                       biogas
7   Biomassa (hout binnenland)
8   Biomassa (hout buitenland)
9                       Diesel
10               Elektriciteit
11                Heat100to200
12                    Methanol
13                      Warmte
14                   Waterstof


                        DoelProduct
0                              None
1                           Aardgas
2                 Aardgas feedstock
3                           Benzine
4                   Biobrandstoffen
5                            biogas
6                            BioHFO
7                       Biokerosine
8              Bio-LNG for shipping
9                   Biomassa (hout)
10     Brandstofmix personenvervoer
11                           Diesel
12                    Elektriciteit
13                     Heat100to200
14                     Heat200to400
15                  HeatDir200to400
16                        HeatHT400
17                              HFO
18                              HVC
19                         Methanol
20                           Naphta
21            Plastic Pyrolysis oil
22  Synthetic methanol for shipping
23                           warmte
24                        Waterstof

'''
def map_esdl_carrier_to_opera_equivalent(carrier: str):
    if carrier:
        carrier = carrier.lower().strip()
        if carrier == "electriciteit" or carrier == "electricity":
            return "Elektriciteit"
        elif carrier == "waterstof" or carrier == 'hydrogen' or carrier == 'h2':
            return "Waterstof"
        elif carrier == "aardgas" or carrier == "natural gas" or carrier == "gas":
            return "Aardgas"
        elif carrier == 'warmte' or carrier == "heat":
            return "Warmte"
        elif carrier == 'biomassa' or carrier == 'biomass':
            return "Biomassa (hout binnenland)"
        elif carrier == 'biogas':
            return "biogas"
        else:
            print(f"Don't know how to map carrier {carrier} to an Opera equivalent")
            return carrier

def esdl_category(asset: esdl.EnergyAsset):
    """
    :param asset: esdl EnergyAsset
    :return: Producer, Consumer, Storage, Transport, Conversion
    """
    super_types = [eclass.name for eclass in asset.eClass.eAllSuperTypes()]
    return super_types[super_types.index(esdl.EnergyAsset.eClass.name) - 1]



