import shutil

import pandas as pd
import pandas.io.sql as psql
import sqlalchemy as sa

from tno.shared.log import get_logger

log = get_logger(__name__)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)


def opera_energycarrier(carrier):
    if pd.isna(carrier):
        return None
    else:
        return 'MMvIB_' + carrier


def activity_name(activity):
    return 'Activity_' + activity


def copy_clean_access_database(emtpy_source, target_db):
    log.info(f"Copying empty Opera DB to target {target_db}")
    shutil.copy2(emtpy_source, target_db)


class OperaAccessImporter:
    year: int = 2030
    scenario = 'MMvIB'
    default_sector = 'Energie'
    df: pd.DataFrame = None  # df with ESDL as a table
    engine = None  # db engine
    conn = None  # db connection
    cursor = None  # db cursor
    not_consumer_options: pd.DataFrame = None
    consumer_options: pd.DataFrame = None

    def init(self, year=2030, scenario='MMvIB', default_sector="Energie"):
        self.year = year
        self.scenario = scenario
        self.default_sector = default_sector

    def connect_to_access(self, access_file: str):
        #access_file = r'C:\data\git\aimms-adapter\esdl2opera_access\Opties_mmvib.mdb'
        odbc_string = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + access_file + ';'

        print(f"Connecting to database {access_file}")
        connection_url = sa.engine.URL.create(
            "access+pyodbc",
            query={"odbc_connect": odbc_string}
        )
        self.engine = sa.create_engine(connection_url)
        self.engine.connect()
        self.conn = self.engine.raw_connection()
        self.cursor = self.conn.cursor()

    def disconnect(self):
        self.cursor.close()
        self.conn.close()

    def start_import(self, esdl_data_frame: pd.DataFrame, access_database: str):
        self.df = esdl_data_frame
        is_consumer = self.df['category'] == 'Consumer'
        self.not_consumer_options = self.df[~is_consumer]
        self.consumer_options = self.df[is_consumer]

        #self.copy_clean_access_database()
        self.connect_to_access(access_file=access_database)
        self._create_energycarriers()
        self._add_activities()  # first activities, then options
        self._add_options()
        self._update_option_related_tables()
        self.disconnect()
        log.info("Import to Opera finished")

    def _create_energycarriers(self):
        carriers = pd.concat([self.df['carrier_in'], self.df['carrier_out']]).dropna().unique()
        for carrier in carriers:
            if carrier == "": continue
            new_carrier_name = opera_energycarrier(carrier)
            sql = "SELECT * FROM [Energiedragers] WHERE [Energiedrager] = '{}'".format(new_carrier_name)
            df = psql.read_sql(sql, self.engine)
            if df.shape[0] == 0:  # not in table yet, insert
                vraagisaanbod = False
                generiek = False
                basisenergiedrager = False
                electriciteit = False
                warmte = False
                # TODO: use
                price = 0
                if carrier.lower().startswith("ele"):
                    vraagisaanbod = True
                    generiek = True
                    basisenergiedrager = True
                    electriciteit = True
                    price = 10.11  # EUR/MWh
                elif carrier.lower().startswith("hydrogen") or carrier.lower().startswith(
                        "waterstof") or carrier.lower().startswith("h2"):
                    vraagisaanbod = True
                    generiek = True
                    price = 8.34  # EUR/GJ
                elif carrier.lower().startswith("aardgas") or carrier.lower().startswith("natural") or \
                        carrier.lower().startswith("fossil gas") or carrier.lower().startswith("gas"):
                    basisenergiedrager = True
                    price = 6.8  # EUR/GJ (KEV 2020 in EUR 2015)
                elif carrier.lower().startswith("heat") or carrier.lower().startswith("warmte"):
                    vraagisaanbod = True
                    generiek = True
                    warmte = True
                else:
                    print(f"EnergyCarrier {carrier} is not matched to a similar energy carrier in Opera, using default values")

                print(f"Inserting new Energy carrier {new_carrier_name}")
                sql = f"INSERT INTO [Energiedragers] ([Energiedrager],[Eenheid],[VraagIsAanbod], [Generiek], [Basisenergiedrager], [Elektriciteit], [Warmte]) " \
                      f" VALUES ('{new_carrier_name}', 'PJ', {vraagisaanbod}, {generiek}, {basisenergiedrager}, {electriciteit}, {warmte});"
                print(sql)
                self.cursor.execute(sql)

                if price != 0.0:
                    print(f"Inserting Energy carrier price {price} for {new_carrier_name}")
                    sql = f"INSERT INTO [EconomieNationaal(Energiedrager,Jaar,Scenario)] ([Energiedrager],[Jaar],[Scenario], [Nationale prijs]) " \
                          f" VALUES ('{new_carrier_name}', {self.year}, '{self.scenario}', {price});"
                    print(sql)
                    self.cursor.execute(sql)
            else:
                print(f"Energy carrier {new_carrier_name} already present")
        self.conn.commit()

    def _add_activities(self):
        for index, row in self.consumer_options.iterrows():
            activiteiten_name = activity_name(row['name'])
            eenheid = 'PJ'
            sql = "SELECT * FROM [Activiteiten] WHERE [Activiteit] = '{}'".format(activiteiten_name)
            df = psql.read_sql(sql, self.engine)
            if df.shape[0] == 0:  # Case where new activity is NOT in table 'Activiteiten'
                print(f'Adding {activiteiten_name} to [Activiteiten]')

                sql = f"INSERT INTO [Activiteiten] ([Activiteit],[Eenheid]) VALUES ('{activiteiten_name}', '{eenheid}' )"
                self.cursor.execute(sql)
            else:
                print(f"{activiteiten_name} already in [Activiteiten]")

            # TODO: add to ActiviteitBaseline(activiteit,scenario,jaar) the annual demand
            sql = "SELECT * FROM [ActiviteitBaseline(activiteit,scenario,jaar)] WHERE [Activiteit] = '{}' AND [Scenario] = '{}' AND [Jaar] = {}" \
                .format(activiteiten_name, self.scenario, self.year)
            df = psql.read_sql(sql, self.engine)
            if df.shape[0] == 0:  # Case where new activity is NOT in table 'ActiviteitBaseline'
                print(f'Adding {activiteiten_name} to [ActiviteitBaseline]')
                #value = row['power'] if not pd.isna(row['power']) else 0.0
                # for activities: use InPort profile
                value = row['profiles_in'] if not pd.isna(row['profiles_in']) else 0.0
                sql = f"INSERT INTO [ActiviteitBaseline(activiteit,scenario,jaar)] ([Activiteit],[Scenario], [Jaar], [Waarde]) VALUES " \
                      f"('{activiteiten_name}', '{self.scenario}', {self.year}, {value} )"
                self.cursor.execute(sql)
            else:
                print(f"{activiteiten_name} already in [ActiviteitBaseline]")

        self.conn.commit()

    def _add_options(self):
        ## Add option to Opties table
        for index, row in self.df.iterrows():
            new_opt = row['name']
            sql = "SELECT * FROM [Opties] WHERE [Naam optie] = '{}'".format(new_opt)
            df = psql.read_sql(sql, self.engine)
            ref_option_name = row.opera_equivalent
            if df.shape[0] == 0:  # Case where new option is NOT in table 'Opties'
                print(f'Adding {new_opt} to [Opties]')

                sql = "SELECT * FROM [Opties] WHERE [Naam optie] = '{}'".format(ref_option_name)
                df_ref_option = psql.read_sql(sql, self.engine)
                # TODO: if no ref_option found, create row ourselves
                df_ref_option = df_ref_option.drop('Nr', axis=1)
                df_ref_option['Naam optie'] = '{}'.format(new_opt)
                df_ref_option['Sector'] = 'Energie'  # use an unused sector in opera for now (see Sectoren table)
                if row['category'] == 'Consumer':
                    df_ref_option['Unit of Capacity'] = '{}'.format('PJ')
                    df_ref_option['Eenheid activiteit'] = '{}'.format('PJ')
                    df_ref_option['Cap2Act'] = 1 # Cap2Act is a number in DB
                    df_ref_option['Optie onbeperkt'] = True
                    df_ref_option['Capaciteit onbeperkt'] = True

                else:
                    df_ref_option['Unit of Capacity'] = '{}'.format('GW')
                    df_ref_option['Eenheid activiteit'] = '{}'.format('PJ')
                    df_ref_option['Cap2Act'] = 31.536
                col = [[i] for i in df_ref_option.columns]

                self.cursor.executemany(
                    'INSERT INTO [Opties] ({}) VALUES ({}{})'.format(str(col)[1:-1],
                                                                     '?,' * (len(df_ref_option.columns) - 1),
                                                                     '?').replace("'", ""),
                    list(df_ref_option.itertuples(index=False, name=None)))

            else:
                print(f"{new_opt} ({df.Nr.values}) already in [Opties]")

        self.conn.commit()

    def _update_option_related_tables(self):
        # add column to self.df with Optie number (Nr)
        self.df['Nr'] = 0
        for index, row in self.df.iterrows():
            new_opt = row['name']
            ref_option_name = row.opera_equivalent
            sql = "SELECT * FROM [Opties] WHERE [Naam optie] = '{}'".format(new_opt)
            df_optie = psql.read_sql(sql, self.engine)
            new_optie_nr = int(df_optie.Nr)
            row['Nr'] = new_optie_nr

            ## Add option to Beschikbare varianten table
            sql = "SELECT * FROM [Beschikbare varianten] WHERE [Nr] = {}".format(new_optie_nr)
            df = psql.read_sql(sql, self.engine)

            if ref_option_name is not None and not pd.isna(ref_option_name):
                sql = "SELECT * FROM [Opties] WHERE [Naam optie] = '{}'".format(ref_option_name)
                df_ref_option = psql.read_sql(sql, self.engine)
            else:
                df_ref_option = None

            if df.shape[0] == 0:  # Case where new option is NOT in table 'Beschikbare varianten'

                if df_ref_option is not None:
                    # copy data from the reference [Beschikbare varianten] and use that to insert new option
                    sql = "SELECT * FROM [Beschikbare varianten] WHERE [Nr] = {}".format(int(df_ref_option.Nr))
                    df3 = psql.read_sql(sql, self.engine)
                    df3.Nr = df_optie.Nr
                    col = [[i] for i in df3.columns]

                    q = 'INSERT INTO [Beschikbare varianten] ({}) VALUES ({}{})'.format(str(col)[1:-1], '?,' * (
                            len(df3.columns) - 1), '?').replace("'", "")
                    values = list(df3.itertuples(index=False, name=None))
                    print(q, values)
                    self.cursor.executemany(q, values)
                else:
                    print(f"Adding new option {df_optie['Naam optie'].values} to [Beschikbare varianten]")
                    # insert using defaults
                    q = f'INSERT INTO [Beschikbare varianten] ([Nr], [Variant], [Beschikbaar]) VALUES ({new_optie_nr}, 1, 1)'
                    print(q)
                    res = self.cursor.execute(q)
                    print(res)
                self.conn.commit()

            else:
                print(f'Option {df_optie.Nr.values}/{new_opt} is already in [Beschikbare varianten]')
                print(df)

            ## Add option to Kosten table

            sql = "SELECT * FROM [Kosten(Optie,Variant,Jaar)] WHERE [Nr] = {} AND [Jaar] = '{}'".format(new_optie_nr,
                                                                                                        self.year)
            df = psql.read_sql(sql, self.engine)
            if df.shape[0] == 0:  # Case where new option is NOT in table 'Kosten'
                investment_cost = row['investment_cost'] if not pd.isna(row['investment_cost']) else 0.0
                o_m_cost = row['o_m_cost'] if not pd.isna(row['o_m_cost']) else 0.0
                if df_ref_option is not None:
                    sql = "SELECT * FROM [Kosten(Optie,Variant,Jaar)] WHERE [Nr] = {} AND [Jaar] = '{}'".format(
                        int(df_ref_option.Nr),
                        self.year)
                    df3 = psql.read_sql(sql, self.engine)
                    df3.Nr = df_optie.Nr
                    df3['Investeringskosten'] = float(investment_cost)
                    df3['Overig operationeel kosten/baten'] = float(o_m_cost)
                    # Do we need to add more costs (?)
                    col = [[i] for i in df3.columns]

                    self.cursor.executemany(
                        'INSERT INTO [Kosten(Optie,Variant,Jaar)] ({}) VALUES ({}{})'.format(str(col)[1:-1], '?,' * (
                                len(df3.columns) - 1), '?').replace("'", ""),
                        list(df3.itertuples(index=False, name=None)))
                else:
                    # TODO add overige kosten of variabele kosten?
                    q = f'INSERT INTO [Kosten(Optie,Variant,Jaar)] (Nr, Variant, Jaar, Investeringskosten, Overig operationeel kosten/baten) ' \
                        f'VALUES ({new_optie_nr}, 1, {self.year}, {float(investment_cost)}, {float(o_m_cost)})'
                    self.cursor.execute(q)
                self.conn.commit()
            else:
                print(
                    f'Option {df_optie.Nr.values}/{new_opt} has already costs attached in [Kosten(Optie,Variant,Jaar)]')
                print(df)

            # Add option to Energiegebruik table, update efficiency in  Effect column (x unit required for 1 unit of output)
            # first input carriers
            carrier_in = row['carrier_in']
            if carrier_in is not None and carrier_in:
                carrier_in = opera_energycarrier(carrier_in)  # convert to Opera version of this ESDL carrier
                sql = "SELECT * FROM [Energiegebruik(Optie,Energiedrager,Variant,Jaar)] WHERE [Nr] = {} AND [Jaar] = '{}' AND [Energiedrager] = '{}'".format(
                    new_optie_nr, self.year, carrier_in)
                df = psql.read_sql(sql, self.engine)
                if df.shape[0] == 0:  # Case where new option is NOT in table 'Energiegebruik'
                    print(f"Inserting efficiency for option {new_opt} and carrier_in {carrier_in}")
                    sql = f"INSERT INTO [Energiegebruik(Optie,Energiedrager,Variant,Jaar)] ([Nr],[Energiedrager],[Variant],[Jaar],[Effect]) VALUES " \
                          f"({new_optie_nr}, '{carrier_in}', {1}, {self.year}, '{1}');"  # Effect for consumption is always 1
                    print(sql)
                    self.cursor.execute(sql)
                else:
                    print(
                        f'Option {df_optie.Nr.values}/{new_opt} is already present in [Energiegebruik(Optie,Energiedrager,Variant,Jaar)]')

            carrier_out = row['carrier_out']
            if carrier_out is not None and carrier_out:
                carrier_out = opera_energycarrier(carrier_out)  # convert to opera equivalent of this ESDL carrier
                sql = "SELECT * FROM [Energiegebruik(Optie,Energiedrager,Variant,Jaar)] WHERE [Nr] = {} AND [Jaar] = '{}' AND [Energiedrager] = '{}'".format(
                    new_optie_nr, self.year, carrier_out)
                df = psql.read_sql(sql, self.engine)
                if df.shape[0] == 0:  # Case where new option is NOT in table 'Energiegebruik'
                    print(f"Inserting efficiency for option {new_opt} and carrier_out {carrier_out}")
                    # Effect is a 'Short Text' column. insert as string and use . as decimal separator instead of ,
                    effect = -row['efficiency'] if row['efficiency'] != 0.0 or not pd.isna(row['efficiency']) else -1
                    sql = f"INSERT INTO [Energiegebruik(Optie,Energiedrager,Variant,Jaar)] ([Nr],[Energiedrager],[Variant],[Jaar],[Effect]) VALUES " \
                          f"({new_optie_nr}, '{carrier_out}', {1}, {self.year}, '{str(effect)}');"
                    self.cursor.execute(sql)
                else:
                    print(
                        f'Option {df_optie.Nr.values}/{new_opt} is already present in [Energiegebruik(Optie,Energiedrager,Variant,Jaar)]')

            ## Add option to CatJaarScen table
            sql = "SELECT * FROM [CatJaarScen(categorie,jaar,scenario)] WHERE [Categorie] = '{}' AND [Jaar] = '{}' AND [Scenario] = '{}'".format(
                new_optie_nr, self.year, self.scenario)
            df = psql.read_sql(sql, self.engine)
            if df.shape[0] == 0:  # Case where new option is NOT in table 'CatJaarScen'
                sql = "SELECT * FROM [CatJaarScen(categorie,jaar,scenario)] WHERE [Categorie] = '{}' AND [Jaar] = '{}' AND [Scenario] = '{}'".format(
                    int(df_ref_option.Nr), self.year, self.scenario)
                df3 = psql.read_sql(sql, self.engine)
                if not df3.empty:  # can use reference option
                    print(f"Adding new CatJaarScen for optie {new_optie_nr}/{new_opt}, based on reference option {ref_option_name}")
                    df3.Categorie = new_optie_nr
                    df3['Max totale capaciteit'] = row['power_max'] if not pd.isna(row['power_max']) else None
                    df3['Min totale capaciteit'] = row['power_min'] if not pd.isna(row['power_min']) else 0

                    col = [[i] for i in df3.columns]
                    sql = 'INSERT INTO [CatJaarScen(categorie,jaar,scenario)] ({}) VALUES ({}{})'.format(str(col)[1:-1], '?,' * (len(df3.columns) - 1), '?').replace("'", "")
                    #print(sql)
                    #print(df3)
                    #print(list(df3.itertuples(index=False, name=None)))
                    self.cursor.executemany(
                        sql,
                        list(df3.itertuples(index=False, name=None)))
                else:
                    print(f"Adding new CatJaarScen for optie {new_optie_nr}/{new_opt}")
                    max_capacity = row['power_max'] if not pd.isna(row['power_max']) else 0 #None
                    min_capacity = row['power_min'] if not pd.isna(row['power_min']) else 0
                    # currently not filling in columns [Max aantal], [Max kosten], [Min aantal], [Min kosten],
                    sql = f'INSERT INTO [CatJaarScen(categorie,jaar,scenario)] ([Categorie], [Jaar], [Scenario], [Max totale capaciteit], [Min totale capaciteit]) ' \
                          f"VALUES ('{new_optie_nr}', '{self.year}', '{self.scenario}', {max_capacity}, {min_capacity});"
                    #print(sql)
                    self.cursor.execute(sql)

                self.conn.commit()

            else:
                print(
                    f'Option {df_optie.Nr.values}/{new_opt} is already present in [CatJaarScen(categorie,jaar,scenario)] with scenario {self.scenario}')
                print(df)

            if row['category'] == 'Consumer':
                activiteiten_name = activity_name(row['name'])
                # TODO: OptieActiviteit(Optie,Activiteit) : connect option to activiteit.
                sql = "SELECT * FROM [OptieActiviteit(Optie,Activiteit)] WHERE [Optie] = {} AND [Activiteit] = '{}'" \
                    .format(new_optie_nr, activiteiten_name)
                df = psql.read_sql(sql, self.engine)
                if df.shape[0] == 0:  # Case where new activity is NOT in table 'ActiviteitBaseline'
                    print(f'Adding {activiteiten_name} to [OptieActiviteit]')
                    sql = f"INSERT INTO [OptieActiviteit(Optie,Activiteit)] ([Optie],[Activiteit], [Match]) VALUES " \
                          f"({new_optie_nr}, '{activiteiten_name}', {True})"
                    self.cursor.execute(sql)
                else:
                    print(f"{activiteiten_name} already in [OptieActiviteit]")

            ### A similar procedure can be done to include activities (demands)
            # following slide "Database (5)" in the file "D:\MMVIB\MMvIB\Working with OPERA_20220907.pptx"
            # It is crucial that the table 'OptieActiviteit' connect (match) the number of the included Options and an Activiteit (demand) in OPERA

        self.conn.commit()
