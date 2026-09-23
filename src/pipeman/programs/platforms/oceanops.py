from urllib.parse import urlencode

import requests

from pipeman.programs.platforms.base import PlatformInfo, PlatformManager

url_args = {
    "include": "[" + ",".join(f'"{x}"' for x in (
        "ptfIdentifiers",
        "agencyPtfs",
        "agencyPtfs.agency",
        "agencyPtfs.agency.country",
        "agencyPtfs.agencyRole",
        'wmos',
        "ptfModel",
        "ptfModel.ptfType",
        "ptfModel.ptfType.ptfFamily",
    )) + "]"
}
OCEAN_OPS_PLATFORMS_URL = "https://www.ocean-ops.org/api/1/data/platform/?" + urlencode(url_args)

def download_ocean_ops():
    with PlatformManager() as manager:
        with requests.Session() as session:
            resp = session.get(OCEAN_OPS_PLATFORMS_URL)
            resp.raise_for_status()
            data = resp.json()
            if "data" in data:
                for platform in data["data"]:
                    for pinfo in build_platform_info(platform):
                        manager.save_platform(pinfo)


def _build_platform_model_info(model_code: str, type_code: str, family_code: str) -> dict:
    kwargs = {}
    if family_code == "FLOAT":
        kwargs["platform_type"] = "profiling_float"
    elif family_code == "MB":
        kwargs["platform_type"] = "moored_buoy"
    elif family_code == "FIXED":
        kwargs["platform_type"] = "fixed"
    elif family_code == "DB":
        kwargs["platform_type"] = "drifting_buoy"
    elif family_code == "GLIDER":
        kwargs["platform_type"] = "glider"
    elif family_code == "TIDE_GAUGE":
        kwargs["platform_type"] = "tide_gauge"
    elif family_code == "ANIMAL":
        kwargs["platform_type"] = "animal"
    elif family_code == "SHIP_WS":
        kwargs["platform_type"] = "ship"
    elif family_code == "CABLE":
        kwargs["platform_type"] = "cable"
    elif family_code == "ATM_P":
        kwargs["platform_type"] = "atmospheric_profiler"
    elif family_code == "CTD":
        kwargs["platform_type"] = "ship"
    elif family_code == "HF_RADAR":
        kwargs["platform_type"] = "radar"
    elif family_code == "UND":
        kwargs["platform_type"] = "ship"
    elif family_code == "USV":
        kwargs["platform_type"] = "usv"
    elif family_code == "AUV":
        kwargs["platform_type"] = "auv"
    elif not family_code:
        kwargs["platform_type"] = "unknown"
    else:
        raise Exception(f"Unknown platform type: {family_code}, {type_code}, {model_code}")
    return kwargs

def build_platform_info(data: dict):
    ids = data.get("ptfIdentifiers", {}) or {}
    kwargs = {
        "platform_name": data.get("name", None),
        "ship_code": ids.get("seadatanetRef", None),
        "imo_number": ids.get("imoRef", None),
        "wigos_id": ids.get("wigosRef", None),
    }
    model = data.get("ptfModel", {}) or {}
    ptf_type = model.get("ptfType", {}) or {}
    ptf_family = ptf_type.get("ptfFamily", {}) or {}
    kwargs.update(_build_platform_model_info(model.get("nameShort", ""), ptf_type.get("nameShort", ""), ptf_family.get("nameShort", "")))
    for agency in data.get("agencyPtfs", []):
        if agency is None:
            continue
        role = agency.get("agencyRole", {}) or {}
        if role.get("nameShort", "") == "DAC":
            agency2 = agency.get("agency", {}) or {}
            country = agency2.get("country", {}) or {}
            kwargs["country"] = country.get("code2", None)
    dates: list[tuple[str, int]] = []
    for status_info in data.get("ptfPtfStatuses", []):
        code = status_info.get("ptfStatus", {}).get("code", None)
        date = status_info.get("changingDate", None)
        if not date:
            continue
        if code == "registered":
            dates.append((date, 0))
        elif code == "closed":
            dates.append((date, 1))
    wmo_ids = data.get("wmos", [])
    if not wmo_ids:
        # TODO: can we do better start/end dates?
        yield PlatformInfo(**kwargs)
    else:
        for wmo_date in wmo_ids:
            wmo_id = wmo_date["wmo"]
            if wmo_id.startswith("WMO"):
                wmo_id = wmo_id[3:]
            pinfo = {
                "start_date": wmo_date.get("startDate", None),
                "end_date": wmo_date.get("endDate", None),
            }
            if wmo_id.isdigit():
                pinfo["wmo_id"] = wmo_id
            else:
                pinfo["platform_id"] = wmo_id
            pinfo.update(kwargs)
            yield PlatformInfo(**kwargs)
