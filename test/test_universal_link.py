import unittest

from dotenv import load_dotenv
from esdl.esdl_handler import EnergySystemHandler

from tno.aimms_adapter.universal_link.universal_link import UniversalLink


class MyTestCase(unittest.TestCase):
    def test_universal_link(self):
        load_dotenv()
        from tno.aimms_adapter.settings import EnvSettings
        ul = UniversalLink(host=EnvSettings.db_host(), database=EnvSettings.db_name(), user=EnvSettings.db_user(), password=EnvSettings.db_password())
        esh = EnergySystemHandler();
        esh.load_file('MACRO 3.3_with_battery.esdl')
        esdl_string = esh.to_string()
        success, errormsg = ul.esdl_to_db(esdl_string)
        print(success, errormsg)


if __name__ == '__main__':
    unittest.main()
