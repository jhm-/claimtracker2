# Copyright (c) 2026 Welcome North Capital Corp.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
# Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import logging
import pygsheets
import pandas as pd
import sys
from cron_converter import Cron
from sqlalchemy import text, exc
from sqlalchemy.dialects.mysql import insert
from threading import Lock
import arcweb_data
from datetime import datetime

global conn_lock
conn_lock = Lock() # connection is shared among Claimtable objects, so prevent a race condition

global claimtables
claimtables = []

# TODO: legacy shit, get rid of this?
class TableDefinition:
    name = ""
    keyCol = ""
    jurisdictionCol = ""
    required_cols = ["RegDate",
                     "Owner",
                     "Area_ha",
                     "ParcelName",
                     "RegTitleNumber",
                     "NextDueDate"]
    column_map = dict()

class ClaimTable(pygsheets.Spreadsheet):
    """ the claimtable class is an extended class from pygsheets, with added functions to load Spreadsheet data from
        MySQL, update tenure expiry dates, modify rows on the Spreadsheet, and more... """
    def __init__(self, engine, suffix, client, jsonsheet=None, id=None, load_config=True):
        super().__init__(client, jsonsheet, id)
        self.engine = engine
        self.suffix = suffix
        self.sheet1.title = self.title
        self.compact_wks = None # compacted worksheet placeholder
        self.supported_jurisdictions = {"YK": arcweb_data.get_data_YK, "NWT": arcweb_data.get_data_NWT, \
                                        "NU": arcweb_data.get_data_NU, "NV": arcweb_data.get_data_NV, \
                                        "BC": arcweb_data.get_data_BC}
        if load_config:
            self.load_config()

    def _write_dataframe(self, wks, df, start=(1,1), fit=True, copy_head=True):
        """ writes a dataframe to a worksheet, replacing NaN and NaT with empty strings """
        wks.set_dataframe(df.astype(object).fillna("").infer_objects(copy=False), 
                          start, encoding="utf-8", fit=fit, copy_head=copy_head)

    def load_config(self):
        """ load the configuration SQL table that is linked to the claimtable, and update table-specific settings  """
        df = pd.DataFrame()
        query = "SELECT * FROM " + self.title + self.suffix["config"]

        conn_lock.acquire()
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), con=conn)
        except exc.SQLAlchemyError as e:
            logging.error("Unable to read table <%s> into dataframe", self.title)
            logging.error(e)
        finally:
            conn_lock.release()

        # crontab-like schedule definitions for updating expiries and emailing the access_list
        now = datetime.now()
        # TODO: better exception handling
        if df.size > 0:
            try:
                self.update_schedule = Cron(df["UpdateSched"].iloc[0]).schedule(now)
                self.update_schedule_iter = self.update_schedule.next()
                self.email_schedule = Cron(df["EmailSched"].iloc[0]).schedule(now)
                self.email_schedule_iter = self.email_schedule.next()
                # google sheet column order
                column_order = df["ColumnOrder"].iloc[0]
                self.column_order = [c.strip() for c in column_order.split(";")]
                if len(self.column_order) < 2:
                    logging.warning("ColumnOrder for <%s> has fewer than 2 columns, check configuration", self.title)
                compact_order = df["CompactColumnOrder"].iloc[0]
                self.compact_order = [c.strip() for c in compact_order.split(";")]
                self.compact = df["Compact"].iloc[0]
                if self.compact and len(self.compact_order) < 2:
                    logging.warning("CompactColumnOrder for <%s> has fewer than 2 columns, check configuration", \
                                    self.title)
                # pruning (remove expired claims)
                self.prune = df["Prune"].iloc[0]
                # google sheet access list (emails)
                access_list = df["AccessList"].iloc[0]
                self.access_list = [r.strip() for r in access_list.split(";")]
                for email in self.access_list:
                   self.share(email, role="reader", type="user")
            except Exception as e:
                logging.error("Unable to read configuration parameters for <%s>", self.title)
                logging.error(e)
        else:
            # TODO: default settings need to be handled better
            # January 31 on a Monday will next be in 2033 (ie. the default, "* * 31 1 1")
            self.update_schedule = Cron("* * 31 1 1").schedule(now)
            self.update_schedule_iter = self.update_schedule.next()
            self.email_schedule = Cron("* * 31 1 1").schedule(now)
            self.email_schedule_iter = self.email_schedule.next()
            self.access_list = []
            self.column_order = TableDefinition().required_cols
            self.compact_order = None # compaction will fail unless self.compact = 0
            self.compact = 0
            self.prune = 0
            # create a one-row dataframe with the defaults
            defaults = {
                    "ColumnOrder": ";".join(self.column_order),
                    "AccessList": [""],
                    "UpdateSched": ["* * 31 1 1"], 
                    "EmailSched": ["* * 31 1 1"],
                    "Prune": ["0"],
                    "Compact": ["0"],
                    "CompactColumnOrder": [""]
            }
            df = pd.DataFrame(defaults)
            self.write_config(df)
        self.config_df = df # save the dataframe for the raw values of the configuration properties

    def write_config(self, df):
        def mysql_upsert_into(table, conn, keys, data_iter):
            """ custom to_sql method to execute a REPLACE INTO statement ensuring single row with primary key id=1 """
            cols = ["id"] + keys
            cols_sql = ", ".join(cols)
            values_placeholders = [f":{c}" for c in cols]
            values_sql = ", ".join(values_placeholders)
            sql = f"""
                REPLACE INTO {table.name} ({cols_sql}) 
                VALUES ({values_sql})
            """
            params = []
            for row in data_iter:
                param_dict = dict(zip(keys, row))
                param_dict["id"] = 1 
                params.append(param_dict)
                break 
            conn.execute(text(sql), params)

        if not df.empty:
            conn_lock.acquire()
            try:
                with self.engine.connect() as conn:
                    df.to_sql(self.title + self.suffix["config"], conn, index=False, if_exists="append",
                              method=mysql_upsert_into)
            except exc.SQLAlchemyError as e:
                logging.error("Unable to write configuration to table <%s>", self.title + self.suffix["config"])
                logging.error(e)
            finally:
                conn_lock.release()

    def update_config(self, table_properties):
        self.config_df = pd.DataFrame(table_properties)
        self.write_config(self.config_df)
        self.load_config()

    def new(self):
        """ generate a new table to store tenure data in the SQL database, and a table of associated configuration
            settings, by copying the column information from templates """
        df = pd.DataFrame()
        success = False
        query = "DROP TABLE IF EXISTS " + self.title + ";" + \
                    "CREATE TABLE " + self.title + " LIKE _Parcels_Template;" + \
                    "ALTER TABLE " + self.title + " ADD INDEX NextDueDate (NextDueDate);" + \
                    "ALTER TABLE " + self.title + " ADD INDEX RegDate (RegDate);" + \
                    "ALTER TABLE " + self.title + " ADD INDEX UpdateDate (UpdateDate);" + \
                    "DROP TABLE IF EXISTS " + self.title + self.suffix["config"] + ";" + \
                    "CREATE TABLE " + self.title + self.suffix["config"] + " LIKE _Config_Template;" + \
                    "ALTER TABLE " + self.title + self.suffix["config"] + " AUTO_INCREMENT = 1"
        query = query.split(";")
        
        conn_lock.acquire()
        try:
            with self.engine.begin() as conn:
                for q in query:
                    conn.execute(text(q))
                query = "SELECT * FROM " + self.title
                df = pd.read_sql(text(query), con=conn) # Put the columns into the dataframe
            success = True
        except exc.SQLAlchemyError as e:
            logging.error("Unable to create new table <%s>", self.title)
            logging.error(e)
        finally:
            conn_lock.release()

        if not success:
            return

        self.load_config()

        # re-order columns
        df = df[self.column_order + [c for c in df.columns if c not in self.column_order]]
        self._write_dataframe(self.sheet1, df)
        # can't freeze rows when there's only one row
        # self.sheet1.frozen_rows = 1
        self.sheet1.link()

    def rename(self, new_title):
        """ rename the SQL tables and the Claimtable worksheet titles """
        logging.info("Renaming table <%s> -> <%s>", self.title, new_title)
        new_title_config = new_title + self.suffix["config"]
        new_title_compact = new_title + self.suffix["compact"]
        query = "ALTER TABLE " + self.title + " RENAME TO " + new_title + ";" + \
                "ALTER TABLE " + self.title + self.suffix["config"] + " RENAME TO " + new_title_config
        query = query.split(";")

        conn_lock.acquire()
        try:
            with self.engine.begin() as conn:
                for q in query:
                    conn.execute(text(q))
        except exc.SQLAlchemyError as e:
            logging.error("Unable to rename table <%s>", self.title)
            logging.error(e)
            return
        finally:
            conn_lock.release()

        if self.compact_wks is not None:
            conn_lock.acquire()
            query = "ALTER TABLE " + self.title + self.suffix["compact"] + " RENAME TO " + new_title_compact
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(query))
            except exc.SQLAlchemyError as e:
                logging.error("Unable to rename table <%s>", self.title)
                logging.error(e)
                return
            finally:
                conn_lock.release()

        self.title = new_title

    def destroy(self):
        """ drop the SQL table from the database, as well as the associated configuration and compacted tables, and
            then use the pygsheets delete function to disconnect the google sheet and free memory """
        logging.debug("Destroying table <%s>", self.title)
        query = "DROP TABLE IF EXISTS " + self.title + ";" + \
                "DROP TABLE IF EXISTS " + self.title + self.suffix["config"] + ";" + \
                "DROP TABLE IF EXISTS " + self.title + self.suffix["compact"]
        query = query.split(";")

        conn_lock.acquire()
        try:
            with self.engine.begin() as conn:
                for q in query:
                    conn.execute(text(q))
        except exc.SQLAlchemyError as e:
            logging.error("Unable to drop table <%s>", self.title)
            logging.error(e)
        finally:
            conn_lock.release()

        self.delete()

    def update(self, inTable: TableDefinition, jurisdiction: str, RegTitleNumber=None):
        """ update the tenure information by polling the appropriate ArcGIS REST API (see arcweb_data.py) """

        def mysql_replace_into(table, conn, keys, data_iter):
            """ custom to_sql method to INSERT... ON DUPLICATE KEY UPDATE... """
            data = [dict(zip(keys, row)) for row in data_iter]
            stmt = insert(table.table).values(data)
            update_stmt = stmt.on_duplicate_key_update(**dict(zip(stmt.inserted.keys(),
                                                                  stmt.inserted.values())))
            conn.execute(update_stmt)

        if jurisdiction in self.supported_jurisdictions:
            data_func = self.supported_jurisdictions[jurisdiction]
        else:
            raise NotImplementedError

        if not inTable.name:
            inTable.name = self.title
        if not inTable.keyCol:
            inTable.keyCol = "RegTitleNumber"
        if not inTable.jurisdictionCol:
            inTable.jurisdictionCol = "Jurisdiction"

        rows = []
        conn_lock.acquire()
        try:
            with self.engine.connect() as conn:
                if not RegTitleNumber:
                    result = conn.execute(text("SELECT " + inTable.keyCol + ", ProjectName, Comments FROM " + inTable.name + \
                        " WHERE " + inTable.jurisdictionCol + "=\"" + jurisdiction + "\""))
                else:
                    result = conn.execute(text("SELECT " + inTable.keyCol + ", ProjectName, Comments FROM " + inTable.name + \
                        " WHERE " + inTable.jurisdictionCol + "=\"" + jurisdiction + "\" AND RegTitleNumber=\"" + \
                        str(RegTitleNumber) + "\""))
                rows = result.fetchall()
        except exc.SQLAlchemyError as e:
            logging.error("Error retrieving tenure data from table <%s>", self.title)
            logging.error(e)
        finally:
            conn_lock.release()

        # if the list is empty, do not pass go
        if not rows:
            return

        tenure_list = []
        project_list = []
        comment_list = []
        for r in rows:
            tenure_list.append(r[0])
            project_list.append(r[1])
            comment_list.append(r[2])

        # TODO: pop this next bit of code out (minus SQL) as a class method for use outside of a Claimtable object
        tenure_data = data_func(tenure_list)

        for t in tenure_data:
            try:
                idx = tenure_list.index(t["RegTitleNumber"])
            except ValueError:
                logging.warning("Received unexpected RegTitleNumber <%s> from API for table <%s>, skipping",
                                t["RegTitleNumber"], self.title)
                continue

            t["ProjectName"] = project_list[idx]
            t["Jurisdiction"] = jurisdiction
            t["Comments"] = comment_list[idx]
            t["UpdateDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df = pd.DataFrame([t])

            conn_lock.acquire()
            try:
                with self.engine.connect() as conn:
                   if self.prune:
                        for index, row in df.iterrows():
                            if row["NextDueDate"] < datetime.now():
                                logging.debug("Drop parcel <%s> where NextDueDate < datetime.now", \
                                              row["RegTitleNumber"])
                                conn.execute(text("DELETE FROM " + self.title + " WHERE RegTitleNumber=\"" + \
                                             row["RegTitleNumber"] + "\""))
                                df = df.drop(index)
                   if not df.empty:
                       df.to_sql(self.title, conn, index=False, if_exists="append", method=mysql_replace_into)
            except exc.SQLAlchemyError as e:
                logging.error("Error updating expiry dates for table <%s>", self.title)
                logging.error(e)
            finally:
                conn_lock.release()

    def modify_parcel(self, df_before, df_after):
        """ modify a row in the claimtable """
        cell = self.sheet1.find(str(df_before.to_dict()["RegTitleNumber"][0]))
        address = (cell[0].address[0], 0)
        # re-order columns
        df_after = df_after[self.column_order + [c for c in df_after.columns if c not in self.column_order]]
        self._write_dataframe(self.sheet1, df_after, start=address, fit=False, copy_head=False)

    def del_parcel(self, df):
        """ delete a row in the claimtable """
        row = self.sheet1.find(str(df.to_dict()["RegTitleNumber"][0]))[0].row
        self.sheet1.delete_rows(row)

    def add_parcel(self, df):
        """ add a row to the claimtable """
        df = df[self.column_order + [c for c in df.columns if c not in self.column_order]]
        self.sheet1.append_table(df.values.tolist(), start="A1", end=None, dimension="ROWS", overwrite=False)

    def bulk_sync(self):
        """ pulls the current SQL table and pushes the whole thing to GSheets in one call """
        df = pd.DataFrame()

        conn_lock.acquire()
        try:
            with self.engine.connect() as conn:
                query = "SELECT * FROM " + self.title
                df = pd.read_sql(text(query), con=conn)
        except exc.SQLAlchemyError as e:
            logging.error("Database read failed during bulk sync for <%s>", self.title)
            logging.error(e)
            return
        finally:
            conn_lock.release()

        if not df.empty:
            df = df[self.column_order + [c for c in df.columns if c not in self.column_order]]
            self._write_dataframe(self.sheet1, df)

    def load(self):
        """ update expiry dates, load MySQL table into ClaimTable object, run compaction, link with cloud """
        df = pd.DataFrame()
        # load MySQL table into first worksheet
        conn_lock.acquire()
        try:
            with self.engine.connect() as conn:
                query = "SELECT * FROM " + self.title
                df = pd.read_sql(text(query), con=conn)
        except exc.SQLAlchemyError as e:
            logging.critical("FATAL: unable to read table <%s> into dataframe", self.title)
            logging.critical(e)
            sys.exit(1)
        finally:
            conn_lock.release()

        # re-order columns
        df = df[self.column_order + [c for c in df.columns if c not in self.column_order]]
        self._write_dataframe(self.sheet1, df)
        self.sheet1.frozen_rows = 1
        self.sheet1.link()

        self.compaction()

    def compaction(self):
        """ a sort function to group tenures that match in both name and expiry date - in many jurisdictions tenures are
            of a fixed size and are numbered sequentially, and can be lumped together for better legibility """
        if self.compact:
            logging.info("Performing tenure compaction on table <%s>", self.title)
            if self.compact_wks is not None:
                self.del_worksheet(self.compact_wks)
            self.compact_wks = self.add_worksheet(self.title + self.suffix["compact"])
            df = pd.DataFrame()
            with open("compaction_new.sql", "r") as file:
                query = file.read()
                query = query.replace("<!TableName>", self.title)
                query = query.replace("<!Suffix>", self.suffix["compact"])

            conn_lock.acquire()
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("DROP TABLE IF EXISTS " + self.title + self.suffix["compact"]))
                    query = query.split(";")
                    for q in query:
                        conn.execute(text(q))
                    df = pd.read_sql(text("SELECT * FROM " + self.title + self.suffix["compact"]), con=conn)
            except exc.SQLAlchemyError as e:
                logging.error("Unable to generate table compaction for <%s>", self.title)
                logging.error(e)
            finally:
                conn_lock.release()

            # drop and re-order google sheet
            try:
                df = df.drop(columns=["TitleNumberDistance"])
                df = df[self.compact_order + [c for c in df.columns if c not in self.compact_order]]
                self._write_dataframe(self.compact_wks, df)
                self.compact_wks.frozen_rows = 1
            except Exception as e:
                logging.error("Unable to update compaction worksheet for <%s>", self.title)
                logging.error(e)
