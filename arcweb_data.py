# Copyright (c) 2020 Andrew Carne <andrew_carne@icloud.com>
# Updated (c) 2026 Welcome North Capital Corp.
# This file provides convenience functions for retrieving tenure data for NWT, YK, BC and NV using
# ArcGIS web REST APIs provided by the jurisdictions.
# The get_data_XX functions take a list of tenure IDs, and return a list of dict objects standardized to:
#   RegDate (datetime): registration date
#   Owner (string): owner information
#   Area_ha (float): area in hectares
#   ParcelName (string): tenure label
#   RegTitleNumber (string): tenure ID (grant number, etc.)
#   NextDueDate (datetime): next key date (anniversary, expiry, etc.)
#
# Not all jurisdictions provide all information, so some items may be None.
#
# By default the functions run batches of 25 tenure IDs at a time, with a 500 ms delay between requests in order
# to not overload the servers. Queries are made directly via requests with a 30 second socket-level timeout
# rather than via restapi, which does not support reliable timeout control.
#
# For large data sets it may be preferable to break data sets in a wrapper function and handle saving/output
# of data in batches as well, otherwise the final returned list will be very large.
import logging
import requests
import restapi
from datetime import datetime, timedelta
import time

def get_data_NWT(tenure_list):
    """ get tenure data from the Northwest Territories ArcGIS REST API """
    url = "https://www.apps.geomatics.gov.nt.ca/arcgis/rest/services/"
    service_url = "https://www.apps.geomatics.gov.nt.ca/arcgis/rest/services/GNWT/Economy_LCC/MapServer"
    layer = "Active Mineral Claims"
    tenure_filter_col = "CLAIM_NUM"
    cols = ["ANNIV_DT", "AREA_HA", "CANCEL_DT", "CLAIM_NAME", "CLAIM_NUM", "CLAIM_STAT", "DISTRICT", "GROUND_OPEN_DATE",
            "GROUP_NUMBER", "ISSUE_DT", "LAND_CLAIM_AREA", "OWNERS"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    result = []
    for d in data:
        result.append({
            "RegDate": datetime.fromtimestamp(d["ISSUE_DT"]/1000)
                if d["ISSUE_DT"] and d["ISSUE_DT"] > 0 else datetime(1970, 1, 1),
            "Owner": d["OWNERS"],
            "Area_ha": d["AREA_HA"],
            "ParcelName": d["CLAIM_NAME"],
            "RegTitleNumber": d["CLAIM_NUM"],
            "NextDueDate": datetime.fromtimestamp(d["ANNIV_DT"]/1000)
                if d["ANNIV_DT"] and d["ANNIV_DT"] > 0 else datetime(1970, 1, 1)
        })
    return result

def get_data_YK(tenure_list):
    """ get tenure data from the Yukon ArcGIS REST API """
    url = "https://mapservices.gov.yk.ca/arcgis/rest/services/"
    service_url = "https://mapservices.gov.yk.ca/arcgis/rest/services/GeoYukon/GY_Mining/MapServer"
    layer = "Quartz Claims - 50k"
    tenure_filter_col = "GRANT_NUMBER"
    cols = ["CLAIM_LABEL", "DISTRICT_NAME", "EXPIRY_DATE", "GRANT_NUMBER", "OWNER_NAME", "RECORDED_DATE",
            "STAKING_DATE", "SHAPE.AREA"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    result = []
    for d in data:
        result.append({
            "RegDate": datetime.fromtimestamp(d["RECORDED_DATE"] / 1000)
                if d["RECORDED_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["RECORDED_DATE"]/1000),
            "Owner": d["OWNER_NAME"],
            "Area_ha": d["SHAPE.AREA"]/10000,
            "ParcelName": d["CLAIM_LABEL"],
            "RegTitleNumber": d["GRANT_NUMBER"],
            "NextDueDate": datetime.fromtimestamp(d["EXPIRY_DATE"] / 1000)
                if d["EXPIRY_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["EXPIRY_DATE"]/1000)
        })
    return result

def get_data_NV(tenure_list):
    """ get tenure data from the Nevada Division of Minerals ArcGIS REST API """
    url = 'https://services.arcgis.com/CXYUMoYknZtf5Qr3/ArcGIS/rest/services/'
    service_url = "https://services.arcgis.com/CXYUMoYknZtf5Qr3/ArcGIS/rest/services/ArcOnlineNvStateClaims/FeatureServer"
    layer = 'Claim Point Listings'
    tenure_filter_col = "SERIALNUMB"
    cols = ["CLAIMANT", "CLAIMNAME", "LOCDATE", "SERIALNUMB"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    result = []
    for d in data:
        result.append({
            "RegDate": datetime.fromtimestamp(d["LOCDATE"] / 1000)
                if d["LOCDATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["LOCDATE"]/1000),
            "Owner": d["CLAIMANT"],
            "Area_ha": None,
            "ParcelName": d["CLAIMNAME"],
            "RegTitleNumber": d["SERIALNUMB"],
            "NextDueDate": None
        })
    return result

def get_data_BC(tenure_list):
    """ get tenure data from the British Columbia ArcGIS REST API """
    url = 'https://maps.gov.bc.ca/arcserver/rest/services/whse/'
    service_url = 'https://maps.gov.bc.ca/arcgis/rest/services/whse/bcgw_pub_whse_mineral_tenure/MapServer'
    layer = 42
    tenure_filter_col = "TENURE_NUMBER_ID"
    cols = ["AREA_IN_HECTARES", "CLAIM_NAME", "ISSUE_DATE", "GOOD_TO_DATE", "OWNER_NAME", "TENURE_NUMBER_ID"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    result = []
    for d in data:
        result.append({
            "RegDate": datetime.fromtimestamp(d["ISSUE_DATE"] / 1000)
                if d["ISSUE_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["ISSUE_DATE"]/1000),
            "Owner": d["OWNER_NAME"],
            "Area_ha": d["AREA_IN_HECTARES"],
            "ParcelName": d["CLAIM_NAME"],
            "RegTitleNumber": str(d["TENURE_NUMBER_ID"]),
            "NextDueDate": datetime.fromtimestamp(d["GOOD_TO_DATE"] / 1000)
                if d["GOOD_TO_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["GOOD_TO_DATE"]/1000),
        })
    return result

def get_data_NU(tenure_list):
    """ get tenure data from the Nunavut ArcGIS REST API """
    url = 'https://data.aadnc-aandc.gc.ca/geomatics/rest/services/Donnees_Ouvertes-Open_Data/'
    service_url = 'https://data.aadnc-aandc.gc.ca/geomatics/rest/services/Donnees_Ouvertes-Open_Data/Claim_minier_NU_Mineral_Claim/MapServer'
    layer = 0
    tenure_filter_col = "CLAIM_NUM"
    cols = ["AREA_HA", "CLAIM_NUM", "CLAIM_NAME", "ISSUE_DATE", "ANNIV_DT", "OWNERS"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    result = []
    for d in data:
        result.append({
            "RegDate": datetime.fromtimestamp(d["ISSUE_DATE"] / 1000)
                if d["ISSUE_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["ISSUE_DATE"]/1000),
            "Owner": d["OWNERS"],
            "Area_ha": d["AREA_HA"],
            "ParcelName": d["CLAIM_NAME"],
            "RegTitleNumber": d["CLAIM_NUM"],
            "NextDueDate": datetime.fromtimestamp(d["ANNIV_DT"] / 1000)
                if d["ANNIV_DT"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["ANNIV_DT"]/1000),
        })
    return result

def _get_layer_url(base_url, service_url, layer):
    """ uses restapi to resolve the layer URL only — all actual queries bypass restapi """
    ags = restapi.ArcServer(base_url)
    extension = service_url.split('/')[-1]
    if extension == 'MapServer':
        svc = restapi.MapService(service_url, token=ags.token)
    elif extension == 'FeatureServer':
        svc = restapi.FeatureService(service_url, token=ags.token)
    elif extension == 'GPServer':
        svc = restapi.GPService(service_url, token=ags.token)
    elif extension == 'ImageServer':
        svc = restapi.ImageService(service_url, token=ags.token)
    elif extension == 'GeocodeServer':
        svc = restapi.Geocoder(service_url, token=ags.token)
    else:
        raise NotImplementedError('restapi does not support "{}" services!')
    lyr = svc.layer(layer)
    return lyr.url

def get_data(base_url, service_url, layer, tenure_list, tenure_filter_col, out_cols=None, batch_size=25):
    """ wrapper for get_data_slice that iterates data retrieval through a list of tenures, establishing
        the layer URL once and reusing it across all batches, with a 500ms delay between requests in
        order to not overload the server. """
    layer_url = _get_layer_url(base_url, service_url, layer)

    start = 0
    results = list()
    while start < len(tenure_list):
        end = min(start + batch_size, len(tenure_list))
        results += get_data_slice(layer_url, tenure_list[start:end], tenure_filter_col, out_cols)
        start += batch_size
        time.sleep(0.5)
    return results

def get_data_slice(layer_url, tenure_list, tenure_filter_col, out_cols=None, max_retries=3):
    """ performs the query to retrieve the tenure information directly via requests,
        bypassing restapi for reliable timeout control. uses a 30 second socket-level
        timeout and exponential backoff retry on failure. """
    if not out_cols:
        out_cols = "*"
    else:
        out_cols = ",".join(out_cols)
    try:
        where = tenure_filter_col + " IN (" + ','.join([str(int(t)) for t in tenure_list]) + ")"
    except ValueError:
        where = tenure_filter_col + " IN (" + ','.join(["'" + str(t) + "'" for t in tenure_list]) + ")"

    params = {
        "where": where,
        "outFields": out_cols,
        "returnGeometry": "false",
        "f": "json"
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(layer_url + "/query", params=params, timeout=30)
            response.raise_for_status()
            if not response.content:
                raise ValueError("Empty response from ArcGIS server")
            data = response.json()
            if "features" not in data:
                raise ValueError(f"Unexpected response from ArcGIS: {data}")
            return [f["attributes"] for f in data["features"]]
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s then fail
                logging.warning("ArcGIS query failed (attempt %d of %d), retrying in %ds: %s",
                                attempt + 1, max_retries, wait, e)
                time.sleep(wait)
            else:
                logging.error("ArcGIS query failed after %d attempts: %s", max_retries, e)
                raise
