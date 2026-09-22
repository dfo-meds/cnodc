import requests

from pipeman.programs.platforms.base import PlatformInfo, PlatformManager

ICES_API_URL = "https://vocab.ices.dk/services/api/Code/7f9a91e1-fb57-464a-8eb0-697e4b0235b5"
ICES_DETAILS_API_URL = "https://vocab.ices.dk/services/api/codedetail/7f9a91e1-fb57-464a-8eb0-697e4b0235b5/{guid}"


def download_ices_codes():
    with PlatformManager() as manager:
        with requests.Session() as sess:
            resp = sess.get(ICES_API_URL)
            for entry in resp.json():
                resp2 = sess.get(ICES_DETAILS_API_URL.format(guid=entry["guid"]))
                pinfo = parse_ices_entry(entry, resp2.json())
                manager.save_platform(pinfo)


def parse_ices_entry(short: dict, long: dict):
    kwargs = {
        "ship_code": short.get("key", None),
        "platform_name": short.get("description", None),
    }
    for obj in long["attribute"]:
        value = obj.get("value", None)
        if not value:
            continue
        name = obj.get("description", None)
        if name == "Call Sign":
            kwargs["platform_id"] = value
        elif name == "Commissioned":
            kwargs["start_date"] = value
        elif name == "Decommissioned":
            kwargs["end_date"] = value
        elif name == "IMO":
            kwargs["imo_number"] = value
        elif name == "MMSI":
            kwargs["mmsi"] = value

    return PlatformInfo(
        platform_type="ship",
        **kwargs
    )