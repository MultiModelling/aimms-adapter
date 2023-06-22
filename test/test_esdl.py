import unittest
import esdl
from esdl.esdl_handler import EnergySystemHandler

from tno.aimms_adapter.model.opera_accessdb.opera_access_importer import OperaAccessImporter
from tno.aimms_adapter.model.opera_accessdb.results_processor import OperaResultsProcessor
from tno.aimms_adapter.model.opera_esdl_parser.esdl_parser import OperaESDLParser
from tno.aimms_adapter.model.opera_esdl_parser.unit import UnitException

esdl.QuantityAndUnitType.__repr__ = lambda \
    x: f"QaU({x.multiplier if x.multiplier is not esdl.MultiplierEnum.NONE else ''}{x.unit}/{x.perMultiplier if x.perMultiplier is not esdl.MultiplierEnum.NONE else ''}{x.perUnit}{'/' + x.perTimeUnit.name if x.perTimeUnit is not esdl.TimeUnitEnum.NONE else ''})"


class TestESDLParseAndImport(unittest.TestCase):
    def test_unit(self):
        #self.assertEqual(True, False)  # add assertion here
        print(esdl.MultiplierEnum.eLiterals.index(esdl.MultiplierEnum.NONE))
        #factors = [1, 1E-18, 1E-15, 1E-12, 1E-9, 1E-6, 1E-3, 1E-2, 1E-1, 1E1, 1E2, 1E3, 1E6, 1E9, 1E12, 1E15, 1E15,
        #           1E18, 1E21]
        #factor = factors[esdl.MultiplierEnum.eLiterals.index(esdl.MultiplierEnum.MEGA)]
        #print('Factor', factor)

        from tno.aimms_adapter.model.opera_esdl_parser.unit import convert_to_unit, POWER_IN_MW, ENERGY_IN_PJ, ENERGY_IN_MWh
        POWER_IN_kW = esdl.QuantityAndUnitType(description="Power in kW", id="POWER_in_kW",
                                              physicalQuantity=esdl.PhysicalQuantityEnum.POWER,
                                              unit=esdl.UnitEnum.WATT,
                                              multiplier=esdl.MultiplierEnum.KILO)

        value = convert_to_unit(10, POWER_IN_kW, POWER_IN_MW)
        print(f'10 kW is {value} MW')
        try:
            value = convert_to_unit(10, ENERGY_IN_PJ, POWER_IN_MW)
            print(value)
        except UnitException as e:
            print ('Expected error:', e)

        value = convert_to_unit(3600, ENERGY_IN_PJ, ENERGY_IN_MWh)
        print(value)
        print(f'3600 PJ is {value} MWh')

        unit = POWER_IN_kW
        for v in (1, 10, 100, 1000, 10000):
            print(f'{v} {unit.multiplier}{unit.unit} is {convert_to_unit(v, unit, POWER_IN_MW)} {POWER_IN_MW.multiplier}{POWER_IN_MW.unit}')

        temp_in_K = esdl.QuantityAndUnitType(description="Temp in K", id="TemperatureInKelvin",
                                               physicalQuantity=esdl.PhysicalQuantityEnum.TEMPERATURE,
                                               unit=esdl.UnitEnum.KELVIN)
        temp_in_C = esdl.QuantityAndUnitType(description="Temp in K", id="TemperatureInKelvin",
                                               physicalQuantity=esdl.PhysicalQuantityEnum.TEMPERATURE,
                                               unit=esdl.UnitEnum.DEGREES_CELSIUS)

        value = convert_to_unit(20, temp_in_C, temp_in_K)
        value = convert_to_unit(-273, temp_in_C, temp_in_K)
        value = convert_to_unit(0, temp_in_C, temp_in_K)
        print(f'0 degrees C is {value} K')
        value = convert_to_unit(0, temp_in_K, temp_in_C)
        print(f'0 K is {value} *C')


    def test_parser(self):
        parser = OperaESDLParser()
        esh = EnergySystemHandler()
        esh.load_file('MACRO 12b.esdl')
        df = parser.parse(esh.to_string())
        print(df)

    def test_importer(self):
        parser = OperaESDLParser()
        esh = EnergySystemHandler()
        esh.load_file('MACRO 12b.esdl')
        df = parser.parse(esh.to_string())
        print(df)
        oai = OperaAccessImporter()
        access_file = r"C:\data\git\mmvib\opera-adapter\test\Opties_test.mdb"
        oai.start_import(esdl_data_frame=df, access_database=access_file)

    def test_result_parser(self):
        import os
        print(os.getcwd())
        parser = OperaESDLParser()
        esh = EnergySystemHandler()
        esh.load_file('MACRO 7.esdl')
        df = parser.parse(esh.to_string())
        orp = OperaResultsProcessor(output_path='opera/CSV MMvIB 2030', esh=esh, input_df=df)
        orp.update_production_capacities()





if __name__ == '__main__':
    unittest.main()
