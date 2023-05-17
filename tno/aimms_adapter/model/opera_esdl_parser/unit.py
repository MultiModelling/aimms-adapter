import esdl

"""
Convert between esdl units multipliers (not units yet, e.g. Joule to Wh), e.g. MW to kW
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


def equals(base_unit: esdl.QuantityAndUnitType, other: esdl.QuantityAndUnitType) -> bool:
    if base_unit.unit == other.unit and \
            base_unit.multiplier == other.multiplier and \
            base_unit.perUnit == other.per_unit and \
            base_unit.perMultiplier == other.perMultiplier and \
            base_unit.physicalQuantity == other.physicalQuantity:
        return True
    return False


def same_physical_quantity(base_unit: esdl.QuantityAndUnitType, other: esdl.QuantityAndUnitType) -> bool:
    if base_unit.physicalQuantity == other.physicalQuantity and \
            base_unit.unit == other.unit and \
            base_unit.perUnit == other.perUnit:
        return True
    return False


def convert_to_unit(value: float, other: esdl.QuantityAndUnitType, target_unit: esdl.QuantityAndUnitType ) -> float:
    if other is None:
        raise UnitException(f'Missing unit for source conversion: {other}')
    if same_physical_quantity(target_unit, other):
        return convert_multiplier(target_unit, other) * value
    else:
        raise UnitException(f'Unit mismatch inputUnit={other.unit}, toUnit={target_unit.unit}')


def convert_multiplier(base_unit: esdl.QuantityAndUnitType, other: esdl.QuantityAndUnitType) -> float:
    return multipier_value(other.multiplier) / multipier_value(base_unit.multiplier)


def multipier_value(multiplier: esdl.MultiplierEnum):
    # MultiplierEnum
    # ['NONE', 'ATTO', 'FEMTO', 'PICO', 'NANO', 'MICRO',
    #  'MILLI', 'CENTI', 'DECI', 'DEKA', 'HECTO', 'KILO', 'MEGA',
    #  'GIGA', 'TERA', 'TERRA', 'PETA', 'EXA']
    factors = [1, 1E-18, 1E-15, 1E-12, 1E-9, 1E-6, 1E-3, 1E-2, 1E-1, 1E1,
               1E2, 1E3, 1E6, 1E9, 1E12, 1E15, 1E15, 1E18, 1E21]
    return factors[esdl.MultiplierEnum.eLiterals.index(multiplier)]


class UnitException(Exception):
    pass
