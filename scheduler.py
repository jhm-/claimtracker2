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

from claimtable import claimtables
from claimtable import TableDefinition
import configparser
import logging
import threading
from datetime import datetime, timedelta
import pandas as pd
from time import time, sleep
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from sqlalchemy import exc
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

email_template_path = "templates/__email.html"

class Scheduler(threading.Thread):
    """ the Scheduler thread performs three tasks:
        1) connects to the MySQL binlog stream and monitors changes to the claimtable SQL tables, synchronizing with
           google sheets;
        2) updates the tenure expiry dates for each claimtable on a schedule set in its respective configuration;
        3) emits an email status update for each claimtable to a mailing list and schedule set in its respective
           configuration"""

    def __init__(self, configuration):
        super(Scheduler, self).__init__(daemon=True)
        self.stream = None
        self.configuration = configuration

    def prepare_email(self, claimtable):
        """ prepares the body of an email with a table of tenures that have anniversary dates < 4 weeks from today """
        today = datetime.now()
        two_week_expiry = today + timedelta(weeks=4)

        df = claimtable.worksheet().get_as_df()
        df = df[claimtable.column_order + [c for c in df.columns if c not in claimtable.column_order]]

        df["RegDate"] = pd.to_datetime(df["RegDate"])
        df["NextDueDate"] = pd.to_datetime(df["NextDueDate"])
        df["UpdateDate"] = pd.to_datetime(df["UpdateDate"])
        mask = df["NextDueDate"].notna() & (df["NextDueDate"] < two_week_expiry)

        def date_formatter(x):
            # handle timestamps
            if isinstance(x, (datetime, pd.Timestamp)):
                return x.strftime("%Y-%m-%d")
            # return an empty string for missing values
            if pd.isna(x):
                return ""
            # handle all other types
            return str(x)
        formatters = {col: date_formatter for col in df.columns }

        html_content = df[mask].copy().fillna("").to_html(index=False, border=0, classes=None, formatters=formatters)

        # Locate the content between <thead>...</thead> and <tbody>...</tbody> tags
        header_start = html_content.find("<thead>") + len("<thead>")
        header_end = html_content.find("</thead>")
        body_start = html_content.find("<tbody>") + len("<tbody>")
        body_end = html_content.find("</tbody>")

        header_row_content = html_content[header_start:header_end].strip().lstrip("<tr>").rstrip("</tr>").strip()
        # hacky, but drop the first line of header_row_content, because pandas always seems to apply a style to the <tr> tag
        # TODO: can we do this less fragile?
        header_row_content = header_row_content.split("\n")
        header_row_content = "\n".join(header_row_content[1:]).strip()
        body_rows_content = html_content[body_start:body_end].strip()

        if body_rows_content == "":
            body_rows_content = "<tr><td colspan=\"" + str(len(df.columns)) + "\"><i>" + \
                "No tenure anniversary dates before " + two_week_expiry.strftime("%Y-%m-%d") + "</i></td></tr>"

        # exceptions are caught at a higher level
        with open(email_template_path, "r") as f:
            template_html = f.read()

        email_html = template_html.replace("{{ claim-data-columns }}", header_row_content)
        email_html = email_html.replace("{{ claim-data-rows }}", body_rows_content)
        email_html = email_html.replace("{{ google-sheet-url }}", claimtable.worksheet().url)

        return email_html

    def send_email(self, recipients, table_name, email_html):
        """ sends an email of prepared html to a list of recipients using configuration-defined account information """
        smtp_server = self.configuration.get("Emailer", "smtp_server")
        email_account = self.configuration.get("Emailer", "email_account")
        email_password = self.configuration.get("Emailer", "email_password")

        message = MIMEMultipart("alternative")
        message["Subject"] = "Claimtracker update: " + table_name
        message["From"] = email_account

        for r in recipients:
            message["To"] = r
            # replace user placeholder with the recipients email address
            send_html = email_html.replace("{{ user }}", r)
            message.attach(MIMEText(send_html, "html"))

        # exceptions are caught at a higher level
        with smtplib.SMTP(smtp_server, 587) as server:
            server.starttls()
            server.login(email_account, email_password)
            server.sendmail(email_account, recipients, message.as_string())
            logging.info(table_name + ": email successfuly sent to recipient " + r)

    def run(self):
        db = {
                "host": self.configuration.get("Database", "address"),
                "port": int(self.configuration.get("Database", "port")),
                "user": self.configuration.get("Database", "root_user"),
                "password": self.configuration.get("Database", "root_password")
        }
        self.stream = BinLogStreamReader(connection_settings=db, server_id=100, resume_stream=True, \
                                    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], \
                                    enable_logging=False)
        pending_syncs = {}
        SYNC_DELAY = 2 # batch MySQL table changes and synchronize with google sheets every 2 seconds

        # first synchronize changes to the sql table with google sheets
        try:
            while True:
                # updates only in one direction, MySQL->Google Sheet
                binlogevent = self.stream.fetchone()

                if binlogevent:
                    t_name = binlogevent.table
                    # any event (delete, update or write) makes the table out of sync
                    if isinstance(binlogevent, (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent)):
                        pending_syncs[t_name] = time()

                now = time()
                ready_to_finalize = [t for t, last_change_time in pending_syncs.items() \
                                     if (now - last_change_time) >= SYNC_DELAY]
                for t_name in ready_to_finalize:
                    del pending_syncs[t_name]
                    table_obj = next((t for t in claimtables if t.title == t_name), None)
                    if table_obj:
                        try:
                            logging.info("Executing bulk synchronization for table <%s>", t_name)
                            table_obj.bulk_sync()
                        except Exception as e:
                            logging.error("Synchronization cycle failed for <%s>", t_name)
                            logging.error(e)

                # then check the time and date for the update process, email process
                # these functions are blocking; MySQL binlog changes will be backlogged while the process runs below
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
                        try:
                            recipients = table.access_list
                            email_html = self.prepare_email(table)
                            self.send_email(recipients, str(table.title), email_html)
                        except Exception as e:
                            logging.error("Error emailing table expiries for <%s>", e)
                        table.email_schedule_iter = table.email_schedule.next()

                sleep(0.2)

        except exc.SQLAlchemyError:
            logging.critical("FATAL: database connection lost - application shutdown")
            os.kill(os.getpid(), signal.SIGTERM)
            self.stop()

    def stop(self):
        try:
            self.stream.close()
        except AttributeError:
            return
