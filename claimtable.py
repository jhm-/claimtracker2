# Copyright (c) 2025 Welcome North Capital Corp.
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
import time
from cron_converter import Cron
from sqlalchemy import func, text, exc
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

def mysql_replace_into(table, conn, keys, data_iter):
    """ custom to_sql method to INSERT... ON DUPLICATE KEY UPDATE... """
    from sqlalchemy.dialects.mysql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    stmt = insert(table.table).values(data)
    update_stmt = stmt.on_duplicate_key_update(**dict(zip(stmt.inserted.keys(), 
                                               stmt.inserted.values())))

    conn.execute(update_stmt)
    conn.commit() # method doesn't autocommit

class ClaimTable(pygsheets.Spreadsheet):
    """ the claimtable class is an extended class from pygsheets, with added functions to load Spreadsheet data from
        MySQL, update tenure expiry dates, modify rows on the Spreadsheet, and more... """
    def __init__(self, conn, suffix, client, jsonsheet=None, id=None, load_config=True):
        super().__init__(client, jsonsheet, id)
        self.conn = conn
        self.suffix = suffix
        self.sheet1.title = self.title
        self.compact_wks = None # compacted worksheet placeholder
        self.supported_jurisdictions = {"YK": arcweb_data.get_data_YK, "NWT": arcweb_data.get_data_NWT, \
                                        "NU": arcweb_data.get_data_NU, "NV": arcweb_data.get_data_NV, \
                                        "BC": arcweb_data.get_data_BC}
        if load_config:
            self.load_config()

    def load_config(self):
        """ load the configuration SQL table that is linked to the claimtable, and update table-specific settings  """
        df = pd.DataFrame()
        try:
            query = "SELECT * FROM " + self.title + self.suffix["config"]
            conn_lock.acquire()
            df = pd.read_sql(query, con=self.conn)
            conn_lock.release()
        except exc.SQLAlchemyError as e:
            logging.critical("Unable to read table <%s> into dataframe", self.title)
            logging.critcal(e)
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
                self.column_order = column_order.split(";")
                compact_order = df["CompactColumnOrder"].iloc[0]
                self.compact_order = compact_order.split(";")
                # pruning (remove expired claims) and compactions flags
                self.prune = df["Prune"].iloc[0]
                self.compact = df["Compact"].iloc[0]
                # google sheet access list (emails)
                access_list = df["AccessList"].iloc[0]
                self.access_list = access_list.split(";")
                for email in self.access_list:
                   self.share(email, role="reader", type="user")
            except:
                logging.critical("Unable to read configuration parameters for <%s>", self.title)
        else:
            # TODO: default settings need to be handled better
            self.update_schedule = Cron()
            self.update_schedule_iter = None
            self.email_schedule = Cron()
            self.email_schedule.iter = None
            self.access_list = []
            self.column_order = TableDefinition().required_cols
            self.compact_order = None # compaction will fail unless self.compact = 0
            self.compact = 0
            self.prune = 0

    def new(self):
        """ generate a new table to store tenure data in the SQL database, and a table of associated configuration
            settings, by coping the column information from templates """
        df = pd.DataFrame()
        try:
            query = "DROP TABLE IF EXISTS " + self.title + ";" + \
                        "CREATE TABLE " + self.title + " LIKE _Parcels_Template;" + \
                        "ALTER TABLE " + self.title + " ADD INDEX NextDueDate (NextDueDate);" + \
                        "ALTER TABLE " + self.title + " ADD INDEX RegDate (RegDate);" + \
                        "ALTER TABLE " + self.title + " ADD INDEX UpdateDate (UpdateDate);" + \
                        "DROP TABLE IF EXISTS " + self.title + self.suffix["config"] + ";" + \
                        "CREATE TABLE " + self.title + self.suffix["config"] + " LIKE _Config_Template"
            query = query.split(";")
            conn_lock.acquire()
            for q in query:
                self.conn.execute(text(q))
            query = "SELECT * FROM " + self.title
            df = pd.read_sql(query, con=self.conn) # Put the columns into the dataframe
            conn_lock.release()
        except exc.SQLAlchemyError as e:
            logging.critical("Unable to create new table <%s>", self.title)
            logging.critical(e)

        self.load_config()

        # re-order columns
        df = df[self.column_order + [c for c in df.columns if c not in self.column_order]]
        self.sheet1.set_dataframe(df, (1,1), encoding="utf-8", fit=True)
        # can't freeze rows when there's only one row
        # self.sheet1.frozen_rows = 1
        self.sheet1.link()

    def destroy(self):
        """ drop the SQL table from the database, as well as the associated configuration and compacted tables, and
            then use the pygsheets delete function to disconnect the google sheet and free memory """
        logging.debug("Destroying table <%s>", self.title)
        try:
            query = "DROP TABLE IF EXISTS " + self.title + ";" + \
                    "DROP TABLE IF EXISTS " + self.title + self.suffix["config"] + ";" + \
                    "DROP TABLE IF EXISTS " + self.title + self.suffix["compact"]
            query = query.split(";")
            conn_lock.acquire()
            for q in query:
                self.conn.execute(text(q))
            conn_lock.release()
        except exc.SQLAlchemyError as e:
            logging.critical("Unable to drop table <%s>", self.title)
            logging.critical(e)
        self.delete()

    def update(self, inTable: TableDefinition, jurisdiction: str, RegTitleNumber=None):
        """ update the tenure information by polling the appropriate ArcGIS REST API (see arcweb_data.py) """
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

        conn_lock.acquire()
        if not RegTitleNumber:
            rows = self.conn.execute(text("SELECT " + inTable.keyCol + ", ProjectName, Comments FROM " + inTable.name + \
                " WHERE " + inTable.jurisdictionCol + "=\"" + jurisdiction + "\""))
        else:
            rows = self.conn.execute(text("SELECT " + inTable.keyCol + ", ProjectName, Comments FROM " + inTable.name + \
                " WHERE " + inTable.jurisdictionCol + "=\"" + jurisdiction + "\" AND RegTitleNumber=\"" + \
                str(RegTitleNumber) + "\""))
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

        i = 0
        start = 0
        batch_size = 25
        while start < len(tenure_list):
            end = start + batch_size
            if end > len(tenure_list):
                end = len(tenure_list)

            tenure_data = data_func(tenure_list[start:end])

            for t in tenure_data:
                t["ProjectName"] = project_list[tenure_list.index(t["RegTitleNumber"])]
                t["Jurisdiction"] = jurisdiction
                t["Comments"] = comment_list[tenure_list.index(t["RegTitleNumber"])]
                t["UpdateDate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df = pd.DataFrame([t])
                conn_lock.acquire()
                # 'prune' ie. remove expired 
                if self.prune:
                    for index, row in df.iterrows():
                        if row["NextDueDate"] < datetime.now():
                            logging.debug("Drop parcel <%s> where NextDueDate < datetime.now", \
                                          row["RegTitleNumber"])
                            self.conn.execute(text("DELETE FROM " + self.title + " WHERE RegTitleNumber=\"" + \
                                 row["RegTitleNumber"] + "\""))
                            df = df.drop(index)
                if not df.empty:
                    df.to_sql(self.title, self.conn, index=False, if_exists="append", method=mysql_replace_into)
                conn_lock.release()
                i = i + 1

            time.sleep(0.1)
            start = start + batch_size

    def modify_parcel(self, df_before, df_after):
        """ modify a row in the claimtable """
        cell = self.sheet1.find(str(df_before.to_dict()["RegTitleNumber"][0]))
        address = (cell[0].address[0], 0)
        # re-order columns
        df_after = df_after[self.column_order + [c for c in df_after.columns if c not in self.column_order]]
        self.sheet1.set_dataframe(df_after, address, encoding="utf-8", copy_head=False)

    def del_parcel(self, df):
        """ delete a row in the claimtable """
        row = self.sheet1.find(str(df.to_dict()["RegTitleNumber"][0]))[0].row
        self.sheet1.delete_rows(row)

    def add_parcel(self, df):
        """ add a row to the claimtable """
        df = df[self.column_order + [c for c in df.columns if c not in self.column_order]]
        self.sheet1.append_table(df.values.tolist(), start="A1", end=None, dimension="ROWS", overwrite=False)

    def load(self):
        """ update expiry dates, load MySQL table into ClaimTable object, run compaction, link with cloud """
#        logging.info("Updating expiries for all parcels in <%s>", self.title)
#        for jurisdiction in self.supported_jurisdictions:
#            self.update(TableDefinition(), jurisdiction)

        df = pd.DataFrame()
        # load MySQL table into first worksheet
        try:
            query = "SELECT * FROM " + self.title
            conn_lock.acquire()
            df = pd.read_sql(query, con=self.conn)
            conn_lock.release()
        except exc.SQLAlchemyError as e:
            logging.critical("Unable to read table <%s> into dataframe", self.title)
            logging.critcal(e)

        # re-order columns
        df = df[self.column_order + [c for c in df.columns if c not in self.column_order]]
        self.sheet1.set_dataframe(df, (1,1), encoding="utf-8", fit=True)
        self.sheet1.frozen_rows = 1
        self.sheet1.link()

        self.compaction()

    def compaction(self):
        """ a sort function to group tenures that match in both name and expiry date - in many jurisdictions tenures are
            of a fixed size and are numbered sequentially, and can be lumped together for better legibility """
        if self.compact:
            logging.info("Performing tenure compaction on table <%s>", self.title)
            self.compact_wks = self.add_worksheet(self.title + self.suffix["compact"])
            df = pd.DataFrame()
            with open("compaction_new.sql", "r") as file:
                query = file.read()
                query = query.replace("<!TableName>", self.title)
                query = query.replace("<!Suffix>", self.suffix["compact"])
            try:
                conn_lock.acquire()
                self.conn.execute(text("DROP TABLE IF EXISTS " + self.title + self.suffix["compact"]))
                query = query.split(";")
                for q in query:
                    self.conn.execute(text(q))
                df = pd.read_sql(text("SELECT * FROM " + self.title + self.suffix["compact"]), con=self.conn)
                conn_lock.release()
            except exc.SQLAlchemyError as e:
                logging.error("Unable to generate table compaction for <%s>", self.title)
                logging.error(e)
            # drop and re-order google sheet
            df = df.drop(columns=["Area_ha", "TitleNumberDistance"])
            df = df[self.compact_order + [c for c in df.columns if c not in self.compact_order]]
            self.compact_wks.set_dataframe(df, (1,1), encoding="utf-8", fit=True)
            self.compact_wks.frozen_rows = 1
