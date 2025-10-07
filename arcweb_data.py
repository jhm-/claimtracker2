#!/usr/bin/env python
# Copyright (c) 2020 Andrew Carne <andrew_carne@icloud.com>
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
# By default the functions run batches of 25 tenure IDs at a time, with a 100 ms delay between requests in order
# to not overload the servers.
#
# For large data sets it may be preferable to break data sets in a wrapper function and handle saving/output
# of data in batches as well, otherwise the final returned list will be very large.

import restapi
from datetime import datetime, timedelta
import time

def get_data_NWT(tenure_list):
    url = "https://www.apps.geomatics.gov.nt.ca/arcgis/rest/services/"
    service_url = "https://www.apps.geomatics.gov.nt.ca/arcgis/rest/services/GNWT/Economy_LCC/MapServer"
    layer = "Active Mineral Claims"
    tenure_filter_col="CLAIM_NUM"
    cols = ["ANNIV_DT", "AREA_HA", "CANCEL_DT", "CLAIM_NAME", "CLAIM_NUM", "CLAIM_STAT", "DISTRICT", "GROUND_OPEN_DATE",
            "GROUP_NUMBER", "ISSUE_DT", "LAND_CLAIM_AREA", "OWNERS"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    data = [{
        "RegDate": datetime.fromtimestamp(d["ISSUE_DT"]/1000),
        "Owner": d["OWNERS"],
        "Area_ha": d["AREA_HA"],
        "ParcelName": d["CLAIM_NAME"],
        "RegTitleNumber": d["CLAIM_NUM"],
        "NextDueDate": datetime.fromtimestamp(d["ANNIV_DT"]/1000)
    } for d in data]
    return data


def get_data_YK(tenure_list):
    url= "https://mapservices.gov.yk.ca/arcgis/rest/services/"
    service_url="https://mapservices.gov.yk.ca/arcgis/rest/services/GeoYukon/GY_Mining/MapServer"
    layer="Quartz Claims - 50k"
    tenure_filter_col="GRANT_NUMBER"
    cols = ["CLAIM_LABEL", "DISTRICT_NAME", "EXPIRY_DATE", "GRANT_NUMBER", "OWNER_NAME", "RECORDED_DATE",
            "STAKING_DATE", "SHAPE.AREA"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)

    data = [{
        "RegDate": datetime.fromtimestamp(d["RECORDED_DATE"] / 1000)
            if d["RECORDED_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["RECORDED_DATE"]/1000),
        "Owner": d["OWNER_NAME"],
        "Area_ha": d["SHAPE.AREA"]/10000,
        "ParcelName": d["CLAIM_LABEL"],
        "RegTitleNumber": d["GRANT_NUMBER"],
        "NextDueDate": datetime.fromtimestamp(d["EXPIRY_DATE"] / 1000)
            if d["EXPIRY_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["EXPIRY_DATE"]/1000)
    } for d in data]
    return data


def get_data_NV(tenure_list):
    url = 'https://services.arcgis.com/CXYUMoYknZtf5Qr3/ArcGIS/rest/services/'
    service_url="https://services.arcgis.com/CXYUMoYknZtf5Qr3/ArcGIS/rest/services/ArcOnlineNvStateClaims/FeatureServer"
    layer='Claim Point Listings'
    tenure_filter_col="SERIALNUMB"
    cols = ["CLAIMANT", "CLAIMNAME", "LOCDATE", "SERIALNUMB"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    data = [{
        "RegDate": datetime.fromtimestamp(d["LOCDATE"] / 1000)
            if d["LOCDATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["RECORDED_DATE"]/1000),
        "Owner": d["CLAIMANT"],
        "Area_ha": None,
        "ParcelName": d["CLAIMNAME"],
        "RegTitleNumber": d["SERIALNUMB"],
        "NextDueDate": None
    } for d in data]
    return data


def get_data_BC(tenure_list):
    #url = 'https://maps.gov.bc.ca/arcserver/rest/services/'
    url = 'https://maps.gov.bc.ca/arcserver/rest/services/whse/'
    #service_url = 'https://maps.gov.bc.ca/arcserver/rest/services/mpcm/bcgwpub/MapServer'
    service_url = 'https://maps.gov.bc.ca/arcgis/rest/services/whse/bcgw_pub_whse_mineral_tenure/MapServer'
    layer=42
    tenure_filter_col="TENURE_NUMBER_ID"
    cols = ["AREA_IN_HECTARES", "CLAIM_NAME", "ISSUE_DATE", "GOOD_TO_DATE", "OWNER_NAME", "TENURE_NUMBER_ID"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    data = [{
        "RegDate": datetime.fromtimestamp(d["ISSUE_DATE"] / 1000)
            if d["ISSUE_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["ISSUE_DATE"]/1000),
        "Owner": d["OWNER_NAME"],
        "Area_ha": d["AREA_IN_HECTARES"],
        "ParcelName": d["CLAIM_NAME"],
        "RegTitleNumber": str(d["TENURE_NUMBER_ID"]),
        "NextDueDate": datetime.fromtimestamp(d["GOOD_TO_DATE"] / 1000)
            if d["GOOD_TO_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["GOOD_TO_DATE"]/1000),
    } for d in data]
    return data


def get_data_NU(tenure_list):
    url = 'https://data.aadnc-aandc.gc.ca/geomatics/rest/services/Donnees_Ouvertes-Open_Data/'
    service_url = 'https://data.aadnc-aandc.gc.ca/geomatics/rest/services/Donnees_Ouvertes-Open_Data/Claim_minier_NU_Mineral_Claim/MapServer'
    layer = 0
    tenure_filter_col="CLAIM_NUM"
    cols = ["AREA_HA", "CLAIM_NUM", "CLAIM_NAME", "ISSUE_DATE", "ANNIV_DT", "OWNERS"]
    data = get_data(url, service_url, layer, tenure_list, tenure_filter_col, cols)
    data = [{
        "RegDate": datetime.fromtimestamp(d["ISSUE_DATE"] / 1000)
            if d["ISSUE_DATE"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["ISSUE_DATE"]/1000),
        "Owner": d["OWNERS"],
        "Area_ha": d["AREA_HA"],
        "ParcelName": d["CLAIM_NAME"],
        "RegTitleNumber": d["CLAIM_NUM"],
        "NextDueDate": datetime.fromtimestamp(d["ANNIV_DT"] / 1000)
            if d["ANNIV_DT"] > 0 else datetime(1970, 1, 1) + timedelta(seconds=d["ANNIV_DT"]/1000),
    } for d in data]
    return data

def get_data(base_url, service_url, layer, tenure_list, tenure_filter_col, out_cols=None, batch_size=25):
    start = 0
    results = list()
    while start < len(tenure_list):
        end = start + batch_size
        if end > len(tenure_list):
            end = len(tenure_list)

        results = results + get_data_slice(base_url, service_url, layer, tenure_list[start:end], tenure_filter_col, out_cols)
        start = start + batch_size
        time.sleep(0.1)

    return results

def get_data_slice(base_url, service_url, layer, tenure_list, tenure_filter_col, out_cols=None):
    ags = restapi.ArcServer(base_url)
    if not out_cols:
        out_cols = "*"

    try:
        query = tenure_filter_col + " IN (" + ','.join([str(int(t)) for t in tenure_list]) + ")"
    except ValueError:
        query = tenure_filter_col + " IN (" + ','.join(["'" + str(t) + "'" for t in tenure_list]) + ")"

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
    result = lyr.query(query, out_cols)

    out_data = [r["properties"] for r in result]

    return out_data
