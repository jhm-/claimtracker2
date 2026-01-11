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

from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (QgsProcessing,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterString,
                       QgsProcessingParameterEnum,
                       QgsMapLayerType,
                       QgsAuthManager)
import mysql.connector

class ClaimTrackerSyncTenuresTool(QgsProcessingAlgorithm):
    TABLE_NAME = "TABLE_NAME"
    DB_HOST = "DB_HOST"
    DB_USER = "DB_USER"
    DB_PASS = "DB_PASS"
    DB_NAME = "DB_NAME"
    JURISDICTION = "JURISDICTION"
    MODE = "MODE"

    def name(self): return "sync_claimtracker_claims"
    def displayName(self): return "Add/Delete Mineral Tenures"
    def group(self): return "Claimtracker: Mineral Tenure Tracking"
    def groupId(self): return "claimtracker"
    def createInstance(self): return ClaimTrackerSyncTenuresTool()

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterEnum(
            self.MODE, 
            "Action to perform", 
            options=["Add selected to Project", "Delete selected from Project"], 
            defaultValue=0))

        # TODO: use QGis Authenticator or some other method to save the default parameters
        self.addParameter(QgsProcessingParameterString(self.DB_HOST, "Database Host", defaultValue="[database host]"))
        self.addParameter(QgsProcessingParameterString(self.DB_NAME, "Database Name", defaultValue="[database name]"))
        self.addParameter(QgsProcessingParameterString(self.DB_USER, "Username", defaultValue="[user name]"))
        self.addParameter(QgsProcessingParameterString(self.DB_PASS, "Password", defaultValue="[password]"))
        self.addParameter(QgsProcessingParameterString(self.TABLE_NAME, "Claim Table", defaultValue="[table name]"))
        self.addParameter(QgsProcessingParameterString(self.JURISDICTION, "Jurisdiction (e.g. YK)", defaultValue="[jurisdiction]"))

    def processAlgorithm(self, parameters, context, feedback):
        from qgis.utils import iface

        # get parameter values
        mode_index = self.parameterAsEnum(parameters, self.MODE, context)
        host = self.parameterAsString(parameters, self.DB_HOST, context)
        db_name = self.parameterAsString(parameters, self.DB_NAME, context)
        user = self.parameterAsString(parameters, self.DB_USER, context)
        pw = self.parameterAsString(parameters, self.DB_PASS, context)
        table = self.parameterAsString(parameters, self.TABLE_NAME, context)
        jur = self.parameterAsString(parameters, self.JURISDICTION, context)

        layer = iface.activeLayer()
        if not layer:
            feedback.reportError("CRITICAL: No active layer found.")
            return {"STATUS": "No Layer"}

        features = layer.selectedFeatures()
        if not features:
            feedback.pushInfo("No features selected. Nothing to do.")
            return {"STATUS": "No Selection"}

        parcels = [(str(f.attribute(1)),) for f in features]

        try:
            cnx = mysql.connector.connect(host=host, database=db_name, user=user, password=pw, connect_timeout=5)
            if cnx.is_connected():
                cur = cnx.cursor()

                if mode_index == 0: # add mode
                    feedback.pushInfo(f"Adding {len(parcels)} claims...")
                    query = f"INSERT IGNORE INTO {table} (RegTitleNumber) VALUES (%s)"
                    cur.executemany(query, parcels)
                    setjur = f"UPDATE {table} SET JURISDICTION = %s WHERE JURISDICTION IS NULL"
                    cur.execute(setjur, (jur,))

                else: # delete mode
                    feedback.pushInfo(f"Deleting {len(parcels)} claims...")
                    query = f"DELETE FROM {table} WHERE RegTitleNumber = %s"
                    cur.executemany(query, parcels)

                cnx.commit()
                feedback.pushInfo(f"Successfully processed {len(parcels)} records in MySQL.")
        except Exception as e:
            feedback.reportError(f"DATABASE ERROR: {e}")
        finally:
            if "cnx" in locals() and cnx.is_connected():
                cur.close()
                cnx.close()

        return {"STATUS": "Complete"}

    def postProcessAlgorithm(self, context, feedback):
        from qgis.utils import iface
        from qgis.core import QgsMapLayerTyp

        feedback.pushInfo("Refreshing Viewport Symbology...")

        active_layer = iface.activeLayer()
        if active_layer:
            active_layer.removeSelection()

        for layer in iface.mapCanvas().layers():
            if layer.type() == QgsMapLayerType.VectorLayer:
                layer.dataProvider().forceReload()
                layer.triggerRepaint()

        iface.mapCanvas().refresh()
        feedback.pushInfo("Map updated successfully.")
        return {"STATUS": "Success"}
