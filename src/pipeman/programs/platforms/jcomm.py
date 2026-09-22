import datetime

from medsutil.countries import a3_to_a2
from pipeman.programs.platforms.base import PlatformInfo, PlatformManager


ORG_TO_ALPHA2: dict[str, str | None] = {
    "Meteo France": "FR",
    "German Meteorological Service": "DE",
}


def import_from_jcomm_file(jcomm_file: str):
    with PlatformManager() as manager:
        with open(jcomm_file, "r") as h:
            headers = None
            for idx, line in enumerate(h.readlines(), start=1):
                try:
                    line = line.strip()
                    if not line:
                        continue
                    if headers is None:
                        if line.startswith("WMO;"):
                            headers = [x.strip() for x in line.split(";")]
                    else:
                        data = line.split(';')
                        manager.save_platform(_parse_jcomm_ops_data({
                            headers[k]: data[k] if k < len(data) else ''
                            for k in range(0, len(headers))
                        }))
                except Exception as e:
                    e.add_note(f"Error processing JCOMM ops file on line [{idx}]")
                    raise


def _parse_jcomm_ops_data(jcomm_ops_data: dict[str, str]) -> PlatformInfo:

    # start and end dates of the allocation
    start_date = None
    end_date = None
    if jcomm_ops_data.get("ALLOC_DATE", None):
        start_date = datetime.datetime.fromisoformat(jcomm_ops_data.get("ALLOC_DATE", ""))
    if jcomm_ops_data.get("DEALLOC_DATE", None):
        end_date = datetime.datetime.fromisoformat(jcomm_ops_data.get("DEALLOC_DATE", ""))

    # country identification
    try:
        country_code = jcomm_ops_data.get("COUNTRY", None)
        if country_code:
            if country_code == "U--":
                country = None
            else:
                country = a3_to_a2(country_code)
        elif jcomm_ops_data.get("AGENCY", None):
            country = ORG_TO_ALPHA2[jcomm_ops_data.get("AGENCY", "")]
        else:
            country = None
    except (ValueError, KeyError):
        raise Exception(f"Cannot identify the country for {jcomm_ops_data}")

    # WMO ID (or platform ID if non-numeric)
    wmo_id = jcomm_ops_data.get("WMO", "")

    return PlatformInfo(
        wmo_id=int(wmo_id) if wmo_id.isdigit() else None,
        platform_id=wmo_id if not wmo_id.isdigit() else None,
        start_date=start_date,
        end_date=end_date,
        country=country,
        metadata={
            "jcomm_telecom_id": jcomm_ops_data.get("TELECOM ID", None),
            "jcomm_telecom_system": jcomm_ops_data.get("TELECOM SYSTEM", None),
            "argos_prog": jcomm_ops_data.get("ARGOS_PROG", None),
            "contact_name": jcomm_ops_data.get("CONTACT NAME", None),
            "contact_email": jcomm_ops_data.get("EMAIL", None),
            "contact_program": jcomm_ops_data.get("PROGRAM", None),
            "contact_role": jcomm_ops_data.get("ROLE", None),
            "contact_organization": jcomm_ops_data.get("AGENCY", None),
            "contact_country": jcomm_ops_data.get("COUNTRY", None),
        }
    )
