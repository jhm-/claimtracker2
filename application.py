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

import os
import sys
import argparse
import configparser
import logging
import atexit
import urllib
import sqlalchemy
from sqlalchemy import text, exc
import pygsheets
from claimtable import ClaimTable, claimtables, conn_lock
from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask_wtf.csrf import CSRFProtect, generate_csrf
from threading import Thread
from scheduler import Scheduler

app = Flask(__name__)
app.config["SECRET_KEY"] = "claimtracker"
csrf = CSRFProtect(app)

version = "0.0.3"
config_path = "claimtracker.conf"

class DbDefinition:
    user = ""
    password = ""
    address = ""
    trusted_conn = False
    driver = "{MySQL ODBC 9.2 Unicode Driver}" # modify as neccesary
    database = ""
    port = "3306" # default port

    def connection_string(self):
        if self.trusted_conn:
            params = urllib.parse.quote_plus("DRIVER=" + self.driver + ";" +
                                             "SERVER=" + self.address + ";" +
                                             "PORT=" + self.port + ";" +
                                             "DATABASE=" + self.database + ";" +
                                             "Trusted_Connection=yes")
            return "mysql+pyodbc:///?odbc_connect={}".format(params)
        else:
            params = urllib.parse.quote_plus("DRIVER=" + self.driver + ";" +
                                             "SERVER=" + self.address + ";" +
                                             "PORT=" + self.port + ";" +
                                             "DATABASE=" + self.database + ";" +
                                             "UID=" + self.user + ";" +
                                             "PWD=" + self.password)
        return "mysql+pyodbc:///?odbc_connect={}".format(params)

class Configuration(configparser.RawConfigParser):
    """ implements a configuration parser for an INI-type of language, sets defaults, and validates settings """
    def __init__(self):
        configparser.RawConfigParser.__init__(self)

    def validate(self):
        """ validates the configuration settings"""

        # Validate the logging settings
        if not self.has_section("Logging"):
            self.add_section("Logging")
        if not self.has_option("Logging", "filename"):
            self.set("Logging", "filename", "claimtracker.log")
        try:
            if not int(self.get("Logging", "level")) in range(1,6):
                self.set("Logging", "level", "1")
        except:
            # Out of range or self.has_option fails
            self.set("Logging", "level", "1")
        def loglevel(level):
            return {
                "1": logging.DEBUG,
                "2": logging.INFO,
                "3": logging.WARNING,
                "4": logging.ERROR,
                "5": logging.CRITICAL
            }[level]
        try:
            for h in logging.root.handlers[:]: # Remove any existing handlers
                logging.root.removeHandler(h)
            logging.basicConfig(filename=self.get("Logging", "filename"), \
                                level=loglevel(str(self.get("Logging", "level"))),
                                format="%(asctime)s %(levelname)-8s %(message)s", filemode="w")
        except:
            print("FATAL: Could not write to log\nMake sure <%s> is writable and try again" \
                  % self.get("Logging", "filename"), file=sys.stderr)
            exit()
        # Validate the database settings - how?
        # Validate the credential settings
        if not self.has_section("Credentials"):
            self.add_section("Credentials")
        if not self.has_option("Credentials", "file"):
            self.set("Credentials", "file", "")
        # Validate the table settings
        if not self.has_section("Tables"):
            self.add_section("Tables")
        if not self.has_option("Tables", "config_suffix"):
            self.set("Tables", "config_suffix", "__cnfg")
        if not self.has_option("Tables", "compact_suffix"):
            self.set("Tables", "compact_suffix", "__cmpct")

    def load(self, filename):
        """ reads the configuration file if it exists """
        self.filename = filename
        if os.access(self.filename, os.W_OK):
            self.read(self.filename)

    def save(self, filename):
        """ saves the configuration file """
        try:
            f = open(self.filename, "w")
            self.write(f)
        except(OSError, IOError) as e:
            logging.error("Failed to write the configuration to file " + self.filename)
            logging.error(e)

configuration = Configuration()
configuration.validate() # this sets the defaults before the main execution

# shared among functions - initialize these later
conn = None
#gc = None
suffix = {}

@app.route("/", methods=["GET", "POST"])
@app.route("/<string:table_name>", methods=["GET", "POST"])
def index(table_name=None):
    """ mapped index URL to the index function """
    tables = {}
    selected_url = None

    global claimtables
    for c in claimtables:
        tables[c.title] = c.sheet1.url
    if request.method == "POST":
        selected_table = request.form.get("table_select")
        if selected_table:
            selected_url = tables.get(selected_table)
            return redirect(url_for("index", table_name=selected_table))
    else:
        if table_name and table_name in tables:
            selected_table = table_name
        elif tables:
            selected_table = list(tables.keys())[0]
        selected_url = tables.get(selected_table)
    return render_template("__layout.html", tables=tables.keys(), selected_table=selected_table, \
                           selected_url=selected_url, csrf_token=generate_csrf())

@app.route("/new", methods=["GET", "POST"])
def new():
    """ mapped new (claimtable) URL to the new function """
    data = request.get_json()
    table_name = data.get("table_name")
    if not table_name:
        table_name = "untitled"
    logging.info("New table - connecting with google sheets")

    global claimtables
    try:
        gc = pygsheets.authorize(service_file=configuration.get("Credentials","file"))
        sheet = gc.sheet.create(table_name)
        c = ClaimTable(conn, suffix, gc, sheet, load_config=False)
        c.new()
        claimtables.append(c)
        return jsonify({"success": True, "table_name": table_name, "redirect_url": url_for("index")})
    except Exception as e:
        logging.error("Error creating table: %s", e)
        return jsonify({"success": False, "error" : str(e)})

@app.route("/delete", methods=["GET", "POST"])
def delete():
    """ mapped delete (claimtable) URL to the new function """
    data = request.get_json()
    table_name = data.get("table_name")
    if not table_name:
        table_name = "untitled"
    logging.info("Deleting table: %s", table_name)

    global claimtables
    try:
        for c in claimtables:
            if c.title == table_name:
                c.destroy()
                break
        claimtables = [c for c in claimtables if c.title != table_name]
        return jsonify({"success": True, "table_name": table_name, "redirect_url": url_for("index")})
    except Exception as e:
        logging.error("Error deleting table: %s", e)
        return jsonify({"success": False, "error" : str(e)})

# Not catching signals with the development Werkzeug libary, so use atexit (may change in production?)
def cleanup_on_exit():
    """ application cleanup code """
    print("Caught a Ctrl-C... cleanup")
    for c in claimtables:
        c.delete()

atexit.register(cleanup_on_exit)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", help="specify the host address", default="127.0.0.1")
    args = parser.parse_args()

    configuration.load(config_path)
    configuration.validate()
    logging.info("Claimtracker initialized...")

    db = DbDefinition()
    db.address = configuration.get("Database","address")
    db.port = configuration.get("Database", "port")
    db.database = configuration.get("Database", "database")
    db.user = configuration.get("Database", "user")
    db.password = configuration.get("Database", "password")
    try:
        db_engine = sqlalchemy.create_engine(db.connection_string())
        conn = db_engine.connect()
    except exc.SQLAlchemyError as e:
        logging.critical("Unable to connect to the remote server <%s>", db.address)
        logging.critical(e)

    logging.info("Generating table list from database <%s>", db.database)
    try:
        conn_lock.acquire()
        tables_raw = conn.execute(text("SHOW TABLES"))
        conn_lock.release()
        if  tables_raw is None:
            logging.info("No tables in database!")
            # TODO: we have to exit here, because creating a new table requires _Parcels_Template
            pass
        # '_Parcels_Template' and '_Config_Template' are hard-coded, non-rw
        tables = [t[0] for t in tables_raw if t[0] not in ("_Parcels_Template", "_Config_Template")]
        suffix = {
                "config": configuration.get("Tables","config_suffix"),
                "compact": configuration.get("Tables","compact_suffix")
        }
        # Remove the supplementary tables from the list
        i = 0
        while i < len(tables):
            if tables[i].endswith(suffix["config"]) or tables[i].endswith(suffix["compact"]):
                tables.pop(i)
            else:
                i = i + 1
        logging.debug("Found tables: %s", tables)
    except exc.SQLAlchemyError as e:
        logging.critical("Error fetching data")
        logging.critical(e)

    logging.info("Connecting with google sheets")
    gc = pygsheets.authorize(service_file=configuration.get("Credentials","file"))

    # If the table list is empty, create a new table called 'untitled'
    if not tables:
        logging.info("Empty database; new table: <untitled>")
        new()

    # Load each table into its own ClaimTable object (inherited from the Spreadsheet class)
    # Server is a long-lived application so delete everything from Google Sheets and reload on each new instance
    for t in tables:
        try:
            logging.debug("Deleting existing google spreadsheet: %s", t)
            existing = gc.open(t)
            existing.delete()
        except pygsheets.SpreadsheetNotFound:
            logging.debug("Spreadsheet not found: %s", t)
        except Exception as e:
            logging.critical("Error deleting spreadsheet <%s>", t)
            logging.critical(e)
        try:
            logging.debug("Creating google spreadsheet <%s>", t)
            sheet = gc.sheet.create(t)
            claimtables.append(ClaimTable(conn, suffix, gc, sheet))
        except Exception as e:
            logging.critical("Error creating spreadsheet <%s>", t)
            logging.critical(e)
            logging.critical(e.args)

    logging.info("Loading table data for %s tables into google sheets", str(len(tables)))
    for c in claimtables:
        try:
            c.load()
            logging.debug("Worksheet url for table <%s> : %s", c.title, c.sheet1.url)
        except Exception as e:
            logging.critical(e)

    logging.info("Launching the scheduling thread")
    scheduler = Scheduler(configuration)
    scheduler.start()

    try:
        app.run(host=args.host)
    except (KeyboardInterrupt, SystemExit):
        pass
