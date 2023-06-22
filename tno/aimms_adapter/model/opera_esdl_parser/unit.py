import esdl

"""
Convert between esdl units multipliers , e.g. MW to kW or EUR/kW to MEUR/GW 
including some convertable units, e.g. Joule to Wh and Kelvin to Celcius)
"""

POWER_IN_MW = esdl.QuantityAndUnitType(description="Power in MW", id="POWER_in_MW",
                                       physicalQuantity=esdl.PhysicalQuantityEnum.POWER,
                                       unit=esdl.UnitEnum.WATT,
                                       multiplier=esdl.MultiplierEnum.MEGA)

POWER_IN_GW = esdl.QuantityAndUnitType(description="Power in GW", id="POWER_in_GW",
                                       physicalQuantity=esdl.PhysicalQuantityEnum.POWER,
                                       unit=esdl.UnitEnum.WATT,
                                       multiplier=esdl.MultiplierEnum.GIGA)

POWER_IN_W = esdl.QuantityAndUnitType(description="Power in WATT", id="POWER_in_W",
                                      physicalQuantity=esdl.PhysicalQuantityEnum.POWER,
                                      unit=esdl.UnitEnum.WATT
                                      )

ENERGY_IN_PJ = esdl.QuantityAndUnitType(description="Energy in PJ", id="ENERGY_in_PJ",
                                        physicalQuantity=esdl.PhysicalQuantityEnum.ENERGY,
                                        unit=esdl.UnitEnum.JOULE,
                                        multiplier=esdl.MultiplierEnum.PETA)

ENERGY_IN_J = esdl.QuantityAndUnitType(description="Energy in J", id="ENERGY_in_J",
                                        physicalQuantity=esdl.PhysicalQuantityEnum.ENERGY,
                                        unit=esdl.UnitEnum.JOULE,
                                        multiplier=esdl.MultiplierEnum.NONE)

ENERGY_IN_MWh = esdl.QuantityAndUnitType(description="Energy in MWh", id="ENERGY_in_MWh",
                                         physicalQuantity=esdl.PhysicalQuantityEnum.ENERGY,
                                         unit=esdl.UnitEnum.WATTHOUR,
                                         multiplier=esdl.MultiplierEnum.MEGA)

COST_IN_MEur = esdl.QuantityAndUnitType(description="Cost in MEur", id="COST_in_MEUR",
                                        physicalQuantity=esdl.PhysicalQuantityEnum.COST,
                                        unit=esdl.UnitEnum.EURO,
                                        multiplier=esdl.MultiplierEnum.MEGA)

COST_IN_Eur_per_MWh = esdl.QuantityAndUnitType(description="Cost in €/MWh", id="COST_in_EURperMWH",
                                        physicalQuantity=esdl.PhysicalQuantityEnum.COST,
                                        unit=esdl.UnitEnum.EURO,
                                        perMultiplier=esdl.MultiplierEnum.MEGA,
                                        perUnit=esdl.UnitEnum.WATTHOUR)


COST_IN_MEur_per_GW_per_year = esdl.QuantityAndUnitType(description="Cost in M€/GW/yr", id="COST_in_MEURperGWperYear",
                                        physicalQuantity=esdl.PhysicalQuantityEnum.COST,
                                        multiplier=esdl.MultiplierEnum.MEGA,
                                        unit=esdl.UnitEnum.EURO,
                                        perMultiplier=esdl.MultiplierEnum.GIGA,
                                        perUnit=esdl.UnitEnum.WATT,
                                        perTimeUnit=esdl.TimeUnitEnum.YEAR)
"""Opera operational costs (OPEX) in MEUR/GW/yr"""

COST_IN_MEur_per_GW = esdl.QuantityAndUnitType(description="Cost in M€/GW", id="COST_in_MEURperGW",
                                        physicalQuantity=esdl.PhysicalQuantityEnum.COST,
                                        multiplier=esdl.MultiplierEnum.MEGA,
                                        unit=esdl.UnitEnum.EURO,
                                        perMultiplier=esdl.MultiplierEnum.GIGA,
                                        perUnit=esdl.UnitEnum.WATT)
"""Opera installation cost (CAPEX) in MEUR/GW"""


COST_IN_MEur_per_PJ = esdl.QuantityAndUnitType(description="Cost in M€/PJ", id="COST_in_MEURperPJ",
                                        physicalQuantity=esdl.PhysicalQuantityEnum.COST,
                                        multiplier=esdl.MultiplierEnum.MEGA,
                                        unit=esdl.UnitEnum.EURO,
                                        perMultiplier=esdl.MultiplierEnum.GIGA,
                                        perUnit=esdl.UnitEnum.JOULE)
"""Opera variable cost  in MEUR/PJ"""

def equals(base_unit: esdl.QuantityAndUnitType, other: esdl.QuantityAndUnitType) -> bool:
    if base_unit.unit == other.unit and \
            base_unit.multiplier == other.multiplier and \
            base_unit.perUnit == other.per_unit and \
            base_unit.perMultiplier == other.perMultiplier and \
            base_unit.physicalQuantity == other.physicalQuantity:
        return True
    return False


def convertable(source: esdl.UnitEnum, target: esdl.UnitEnum) -> bool:
    """Checks if a unit can be converted to another unit, e.g. Joule -> Wh or Kelvin -> Celcius"""
    try:
        convert_unit(0, source, target)  # check if convert_unit does not raise an exception
        return True
    except UnitException as e:
        return False


def same_physical_quantity(source: esdl.QuantityAndUnitType, target: esdl.QuantityAndUnitType) -> bool:
    return source.physicalQuantity == target.physicalQuantity \
        and source.perTimeUnit == target.perTimeUnit \
        and (source.unit == target.unit and source.perUnit == target.perUnit) or \
            (convertable(source.unit, target.unit) and convertable(source.perUnit, target.perUnit))


def convert_to_unit(value: float, source_unit: esdl.AbstractQuantityAndUnit, target_unit: esdl.AbstractQuantityAndUnit) -> float:
    if source_unit is None or target_unit is None:
        raise UnitException(f'Missing source unit in unit conversion: source:{source_unit}, target:{target_unit}')
    while isinstance(source_unit, esdl.QuantityAndUnitReference):  # resolve QaU references if necessary
        source_unit = source_unit.reference
    while isinstance(target_unit, esdl.QuantityAndUnitReference):  # resolve QaU references if necessary
        target_unit = target_unit.reference
    if same_physical_quantity(target_unit, source_unit):
        return convert_unit(
            convert_unit(
                convert_multiplier(source_unit, target_unit) * value, source_unit.unit, target_unit.unit), source_unit.perUnit, target_unit.perUnit)

    else:
        raise UnitException(f'Physical quantity mismatch inputUnit={source_unit}, toUnit={target_unit}')


def convert_multiplier(source: esdl.QuantityAndUnitType, target: esdl.QuantityAndUnitType) -> float:
    value = multipier_value(source.multiplier) / multipier_value(target.multiplier) * \
        multipier_value(target.perMultiplier) / multipier_value(source.perMultiplier)
    #print(f"{multipier_value(source.multiplier)} / {multipier_value(target.multiplier)} * {multipier_value(target.perMultiplier)} / {multipier_value(source.perMultiplier)}")
    #print(f"Converting source {source} to {target}: factor={value}")
    return value


    # MultiplierEnum
    # ['NONE', 'ATTO', 'FEMTO', 'PICO', 'NANO', 'MICRO',
    #  'MILLI', 'CENTI', 'DECI', 'DEKA', 'HECTO', 'KILO', 'MEGA',
    #  'GIGA', 'TERA', 'TERRA', 'PETA', 'EXA']
factors = [1, 1E-18, 1E-15, 1E-12, 1E-9, 1E-6, 1E-3, 1E-2, 1E-1, 1E1,
               1E2, 1E3, 1E6, 1E9, 1E12, 1E15, 1E15, 1E18, 1E21]

def multipier_value(multiplier: esdl.MultiplierEnum):
    return factors[esdl.MultiplierEnum.eLiterals.index(multiplier)]


unit_mapping = {
    esdl.UnitEnum.WATTHOUR: {esdl.UnitEnum.JOULE: {'type': 'MULTIPLY', 'value': 3600.0}},
    esdl.UnitEnum.JOULE: {esdl.UnitEnum.WATTHOUR: {'type': 'MULTIPLY', 'value': 1.0/3600.0}},
    esdl.UnitEnum.DEGREES_CELSIUS: {esdl.UnitEnum.KELVIN: {'type': 'ADDITION', 'value': 273.15}},
    esdl.UnitEnum.KELVIN: {esdl.UnitEnum.DEGREES_CELSIUS: {'type': 'ADDITION', 'value': -273.15}}
}

def convert_unit(value: float, source_quantity_unit: esdl.UnitEnum, target_quantity_unit: esdl.UnitEnum) -> float:
    """Does some basic unit conversion, only Joule <> Wh, Wh to Joule and *C to Kelvin and vice versa"""
    # can only covert units when physical quanities are the same (e.g. Energy)
    if source_quantity_unit == target_quantity_unit:
        return value
    else:
        if source_quantity_unit in unit_mapping:
            source_map = unit_mapping[source_quantity_unit]
            if target_quantity_unit in source_map:
                conversion = source_map[target_quantity_unit]
                if conversion['type'] == 'MULTIPLY':
                    #print(f"Unit conversion factor {source_quantity_unit.name} to {target_quantity_unit.name} factor={conversion['value']}")
                    return value * conversion['value']
                elif conversion['type'] == 'ADDITION':
                    return value + conversion['value']
            else:
                UnitException(f"No mapping available from {source_quantity_unit.name} to {target_quantity_unit.name}")
        else:
            UnitException(f"Cannot convert {source_quantity_unit.name} into {target_quantity_unit.name}")


class UnitException(Exception):
    pass
