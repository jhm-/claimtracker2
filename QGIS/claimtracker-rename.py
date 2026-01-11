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
                       QgsMapLayerType)
import mysql.connector

class ClaimTrackerAssignProjectTool(QgsProcessingAlgorithm):
    PROJECT_NAME = "PROJECT_NAME"
    DB_HOST = "DB_HOST"
    DB_USER = "DB_USER"
    DB_PASS = "DB_PASS"
    DB_NAME = "DB_NAME"
    TABLE_NAME = "TABLE_NAME"

    def name(self): return "assign_tenures_to_project"
    def displayName(self): return "Assign Tenures to Project"
    def group(self): return "Claimtracker: Mineral Tenure Tracking"
    def groupId(self): return "claimtracker"
    def createInstance(self): return ClaimTrackerAssignProjectTool()

    def initAlgorithm(self, config=None):
        # TODO: use QGis Authenticator or some other method to save the default parameters
        self.addParameter(QgsProcessingParameterString(self.PROJECT_NAME, "Project Name to Assign", defaultValue="[project name]"))
        self.addParameter(QgsProcessingParameterString(self.DB_HOST, "Database Host", defaultValue="[database host]"))
        self.addParameter(QgsProcessingParameterString(self.DB_NAME, "Database Name", defaultValue="[database name]"))
        self.addParameter(QgsProcessingParameterString(self.DB_USER, "Username", defaultValue="[database]"))
        self.addParameter(QgsProcessingParameterString(self.DB_PASS, "Password", defaultValue="[password]"))
        self.addParameter(QgsProcessingParameterString(self.TABLE_NAME, "Claim Table", defaultValue="[table name]"))

    def processAlgorithm(self, parameters, context, feedback):
        from qgis.utils import iface

        project = self.parameterAsString(parameters, self.PROJECT_NAME, context)
        host = self.parameterAsString(parameters, self.DB_HOST, context)
        db_name = self.parameterAsString(parameters, self.DB_NAME, context)
        user = self.parameterAsString(parameters, self.DB_USER, context)
        pw = self.parameterAsString(parameters, self.DB_PASS, context)
        table = self.parameterAsString(parameters, self.TABLE_NAME, context)

        layer = iface.activeLayer()
        if not layer: return {"STATUS": "No Layer"}

        features = layer.selectedFeatures()
        if not features: return {"STATUS": "No Selection"}

        selected_ids = [str(f.attribute(1)) for f in features]

        try:
            cnx = mysql.connector.connect(host=host, database=db_name, user=user, password=pw, connect_timeout=5)
            cur = cnx.cursor()

            # safety: check which of these IDs actually exist in the database
            format_strings = ",".join(["%s"] * len(selected_ids))
            check_query = f"SELECT RegTitleNumber FROM {table} WHERE RegTitleNumber IN ({format_strings})"
            cur.execute(check_query, tuple(selected_ids)

            existing_ids = [row[0] for row in cur.fetchall()]
            missing_ids = list(set(selected_ids) - set(existing_ids))

            if missing_ids:
                feedback.reportError(f"SAFETY CHECK: {len(missing_ids)} claims are NOT in the database and will be skipped.")
                for m_id in missing_ids:
                    feedback.pushInfo(f"Missing ID: {m_id}")

            if not existing_ids:
                feedback.reportError("Aborting: None of the selected claims exist in the database. Use the "Add" tool first.")
                return {"STATUS": "Failed Safety Check"}

            # only update the ones that exist
            update_data = [(project, x) for x in existing_ids]
            update_query = f"UPDATE {table} SET ProjectName = %s WHERE RegTitleNumber = %s"

            cur.executemany(update_query, update_data)
            cnx.commit()

            feedback.pushInfo(f"SUCCESS: Assigned {len(existing_ids)} claims to project "{project}".")

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

        active_layer = iface.activeLayer()
        if active_layer:
            active_layer.removeSelection()

        for layer in iface.mapCanvas().layers():
            if layer.type() == QgsMapLayerType.VectorLayer:
                layer.dataProvider().forceReload()
                layer.triggerRepaint()

        iface.mapCanvas().refresh()
        return {"STATUS": "Success"}
