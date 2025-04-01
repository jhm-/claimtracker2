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

from claimtable import claimtables
from claimtable import TableDefinition
import configparser
import logging
import threading
from datetime import datetime
import pandas as pd
from time import sleep
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

class Scheduler(threading.Thread):
    def __init__(self, configuration):
        super(Scheduler, self).__init__(daemon=True)
        self.configuration = configuration

    def run(self):
        db = {
                "host": self.configuration.get("Database", "address"),
                "port": int(self.configuration.get("Database", "port")),
                "user": self.configuration.get("Database", "root_user"),
                "password": self.configuration.get("Database", "root_password")
        }
        stream = BinLogStreamReader(connection_settings=db, server_id=100, resume_stream=True, \
                                    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], \
                                    enable_logging=False)
        try:
            while True:
                # updates only in one direction, MySQL->Google Sheet
                # first check the MySQL binlog for changes
                for binlogevent in stream:
                    # binlogevent.table is the table, binlogevent.rows is the row
                    if isinstance(binlogevent, DeleteRowsEvent):
                        logging.debug("DeleteRowsEvent on <%s>", str(binlogevent.table))
                        for row in binlogevent.rows:
                            values = row["values"]
                            df = pd.DataFrame([values])
                            table = next((table for table in claimtables if table.title == binlogevent.table), None)
                            if table:
                                table.del_parcel(df)
                    if isinstance(binlogevent, UpdateRowsEvent):
                        logging.debug("UpdateRowsEvent on <%s>", str(binlogevent.table))
                        for row in binlogevent.rows:
                            before_values = row["before_values"]
                            after_values = row["after_values"]
                            df_before = pd.DataFrame([before_values])
                            df_after = pd.DataFrame([after_values])
                            table = next((table for table in claimtables if table.title == binlogevent.table), None)
                            if table:
                                table.modify_parcel(df_before, df_after)
                    if isinstance(binlogevent, WriteRowsEvent):
                        logging.debug("WriteRowsEvent, on <%s>", str(binlogevent.table))
                        for row in binlogevent.rows:
                            values = row["values"]
                            df = pd.DataFrame([values])
                            table = next((table for table in claimtables if table.title == binlogevent.table), None)
                            if table:
                                table.add_parcel(df)

                # Then check the time and date for the update process, email process
                # These functions are blocking; MySQL binlog changes will be backlogged while the process runs below
                # this could present a race condition - for now it's up to the user to not schedule everything at once
                now = datetime.now()
                for table in claimtables:
                    if not table.update_schedule_iter:
                        continue
                    if table.update_schedule_iter <= now:
                        logging.info("Launching scheduled updater for <%s>", str(table.title))
                        for jurisdiction in table.supported_jurisdictions:
                            table.update(TableDefinition(), jurisdiction)
                        table.compaction()
                        table.update_schedule_iter = table.update_schedule.next()
                    if not table.email_schedule_iter:
                        continue
                    if table.email_schedule_iter <= now:
                        logging.info("Launching scheduled emailer for <%s>", str(table.title))
                        # TODO: prepare body of the email (Feature Not Implemented)
                        df = table.get_as_df()
                        # iterrows or itertuples through df, drop claims with expiries > 1 mo, extract rows out of df
                        # into a new dataframe with expiries <= 1 wk
                        table.email_schedule_iter = table.email_schedule.next()

                sleep(0.2)

        except (KeyboardInterrupt, SystemExit):
            stream.close()
