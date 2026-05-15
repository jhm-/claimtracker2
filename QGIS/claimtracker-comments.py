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
                       QgsProcessingParameterAuthConfig,
                       QgsAuthMethodConfig,
                       QgsMapLayerType)
import mysql.connector

class ClaimTrackerCommentsTool(QgsProcessingAlgorithm):
    COMMENT = "COMMENT"
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

    def name(self): return "update_tenure_comments"
    def displayName(self): return "Update Tenure Comments"
    def group(self): return "Claimtracker: Mineral Tenure Tracking"
    def groupId(self): return "claimtracker"
    def createInstance(self): return ClaimTrackerCommentsTool()

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterString(self.COMMENT, "Comment", defaultValue="", optional=True))
        self.addParameter(QgsProcessingParameterString(self.TABLE_NAME, "Claim Table", defaultValue=""))
        self.addParameter(QgsProcessingParameterString(self.JURISDICTION, "Jurisdiction (e.g. YK)", defaultValue="YK"))
        self.addParameter(QgsProcessingParameterAuthConfig(self.AUTH_CONFIG, "Database Credentials"))

    def processAlgorithm(self, parameters, context, feedback):
        from qgis.utils import iface
        from qgis.core import QgsApplication

        comment = self.parameterAsString(parameters, self.COMMENT, context)
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

        selected_ids = [str(f.attribute(tenure_field)) for f in features]

        try:
            cnx = mysql.connector.connect(host=host, database=db_name, user=user, password=pw, connect_timeout=5)
            cur = cnx.cursor()

            # safety: check which of these IDs actually exist in the database
            format_strings = ",".join(["%s"] * len(selected_ids))
            check_query = f"SELECT RegTitleNumber FROM {table} WHERE RegTitleNumber IN ({format_strings})"
            cur.execute(check_query, tuple(selected_ids))

            existing_ids = [row[0] for row in cur.fetchall()]
            missing_ids = list(set(selected_ids) - set(existing_ids))

            if missing_ids:
                feedback.reportError(f"SAFETY CHECK: {len(missing_ids)} claims are NOT in the database and will be skipped.")
                for m_id in missing_ids:
                    feedback.pushInfo(f"Missing ID: {m_id}")

            if not existing_ids:
                feedback.reportError("Aborting: None of the selected claims exist in the database. Use the 'Add' tool first.")
                return {"STATUS": "Failed Safety Check"}

            # update comment for all existing claims - empty string clears the field
            update_data = [(comment, x) for x in existing_ids]
            update_query = f"UPDATE {table} SET Comments = %s WHERE RegTitleNumber = %s"
            cur.executemany(update_query, update_data)
            cnx.commit()

            if comment:
                feedback.pushInfo(f"SUCCESS: Updated comment for {len(existing_ids)} claims to '{comment}'.")
            else:
                feedback.pushInfo(f"SUCCESS: Cleared comment for {len(existing_ids)} claims.")

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
