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
                       QgsProcessingParameterAuthConfig,
                       QgsAuthMethodConfig,
                       QgsMapLayerType)
import mysql.connector

class ClaimTrackerSyncTenuresTool(QgsProcessingAlgorithm):
    MODE = "MODE"
    TABLE_NAME = "TABLE_NAME"
    JURISDICTION = "JURISDICTION"
    AUTH_CONFIG = "AUTH_CONFIG"

    JURISDICTION_FIELD_MAP = {
        "YK":  "GRANT_NUM",
        "NWT": "CLAIM_NUM",
        "NU":  "CLAIM_NUM",
        "NV":  "SERIALNUMB",
        "BC":  "TENURE_NUMBER_ID"
    }

    def name(self): return "sync_tenures"
    def displayName(self): return "Add or Delete Tenures"
    def group(self): return "Claimtracker: Mineral Tenure Tracking"
    def groupId(self): return "claimtracker"
    def createInstance(self): return ClaimTrackerSyncTenuresTool()

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterEnum(self.MODE, "Mode", options=["Add", "Delete"], defaultValue=0))
        self.addParameter(QgsProcessingParameterString(self.TABLE_NAME, "Claim Table", defaultValue=""))
        self.addParameter(QgsProcessingParameterString(self.JURISDICTION, "Jurisdiction (e.g. YK)", defaultValue="YK"))
        self.addParameter(QgsProcessingParameterAuthConfig(self.AUTH_CONFIG, "Database Credentials"))

    def processAlgorithm(self, parameters, context, feedback):
        from qgis.utils import iface
        from qgis.core import QgsApplication

        mode_index = self.parameterAsEnum(parameters, self.MODE, context)
        table = self.parameterAsString(parameters, self.TABLE_NAME, context)
        jur = self.parameterAsString(parameters, self.JURISDICTION, context)

        auth_config_id = self.parameterAsString(parameters, self.AUTH_CONFIG, context)
        auth_manager = QgsApplication.authManager()
        conf = QgsAuthMethodConfig()
        auth_manager.loadAuthenticationConfig(auth_config_id, conf, True)
        user = conf.config("username")
        pw = conf.config("password")
        host = conf.uri()
        db_name = conf.config("realm")

        layer = iface.activeLayer()
        if not layer:
            feedback.reportError("CRITICAL: No active layer found.")
            return {"STATUS": "No Layer"}

        features = layer.selectedFeatures()
        if not features:
            feedback.pushInfo("No features selected. Nothing to do.")
            return {"STATUS": "No Selection"}

        tenure_field = self.JURISDICTION_FIELD_MAP.get(jur)
        if not tenure_field:
            feedback.reportError(f"CRITICAL: Unknown jurisdiction '{jur}'. Cannot determine tenure ID field.")
            return {"STATUS": "Unknown Jurisdiction"}

        parcels = [(str(f.attribute(tenure_field)),) for f in features]

        try:
            cnx = mysql.connector.connect(host=host, database=db_name, user=user, password=pw, connect_timeout=5)
            if cnx.is_connected():
                cur = cnx.cursor()

                if mode_index == 0:  # add mode
                    feedback.pushInfo(f"Adding {len(parcels)} claims...")
                    query = f"INSERT IGNORE INTO {table} (RegTitleNumber) VALUES (%s)"
                    cur.executemany(query, parcels)
                    # set jurisdiction for new records only
                    setjur = f"UPDATE {table} SET Jurisdiction = %s WHERE Jurisdiction IS NULL"
                    cur.execute(setjur, (jur,))

                else:  # delete mode
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
        from qgis.core import QgsMapLayerType

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
