import unittest
import esdl
from esdl.esdl_handler import EnergySystemHandler

from tno.aimms_adapter.model.opera_esdl_parser.esdl_parser import OperaESDLParser
from tno.aimms_adapter.model.opera_esdl_parser.unit import UnitException


class MyTestCase(unittest.TestCase):
    def test_something(self):
        #self.assertEqual(True, False)  # add assertion here
        print(esdl.MultiplierEnum.eLiterals.index(esdl.MultiplierEnum.NONE))
        factors = [1, 1E-18, 1E-15, 1E-12, 1E-9, 1E-6, 1E-3, 1E-2, 1E-1, 1E1, 1E2, 1E3, 1E6, 1E9, 1E12, 1E15, 1E15,
                   1E18, 1E21]
        factor = factors[esdl.MultiplierEnum.eLiterals.index(esdl.MultiplierEnum.MEGA)]
        print('Factor', factor)

        from tno.aimms_adapter.model.opera_esdl_parser.unit import convert_to_unit, POWER_IN_MW, ENERGY_IN_PJ
        POWER_IN_kW = esdl.QuantityAndUnitType(description="Power in kW", id="POWER_in_kW",
                                              physicalQuantity=esdl.PhysicalQuantityEnum.POWER,
                                              unit=esdl.UnitEnum.WATT,
                                              multiplier=esdl.MultiplierEnum.KILO)

        value = convert_to_unit(10, POWER_IN_kW, POWER_IN_MW)
        print(value)
        try:
            value = convert_to_unit(10, ENERGY_IN_PJ, POWER_IN_MW)
            print(value)
        except UnitException as e:
            print ('Expected error:', e)

        unit = POWER_IN_kW
        range = (0, 100)
        range = tuple([convert_to_unit(v, unit, POWER_IN_MW) for v in range])
        print(range, unit.multiplier, unit.unit)

        parser = OperaESDLParser()
        esh = EnergySystemHandler()
        esh.load_file('MACRO 7.esdl')
        parser.parse(esh.to_string())



if __name__ == '__main__':
    unittest.main()
