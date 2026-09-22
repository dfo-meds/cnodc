import requests
from bs4 import BeautifulSoup

from pipeman.programs.platforms.base import PlatformInfo, PlatformManager

CCGS_PREFIX = "https://inter-j02.dfo-mpo.gc.ca"
CCGS_ACTIVE = "https://inter-j02.dfo-mpo.gc.ca/fdat/vessels"

CCGS_INACTIVE = "https://inter-j02.dfo-mpo.gc.ca/fdat/vessels?status=3"


def download_ccgs_ships():
    with PlatformManager() as manager:
        for pinfo in get_platforms():
            manager.save_platform(pinfo)


def get_platforms():
    with requests.Session() as sess:
        for base_url in (CCGS_ACTIVE, CCGS_INACTIVE):
            resp = sess.get(base_url)
            for vessel_url in get_vessel_urls(resp.content):
                resp2 = sess.get(vessel_url)
                yield get_platform_info(resp2.content)


def get_vessel_urls(content):
    soup = BeautifulSoup(content, "html.parser")
    for table in soup.find_all(id="vesselTable"):
        for row in table.find_all("tr"):
            headers = row.find_all("th")
            if headers:
                continue
            cells = row.find_all("td")
            yield f"{CCGS_PREFIX}{cells[0].find_all("a")[0].get("href")}"


def get_platform_info(content):
    kwargs = {}
    soup = BeautifulSoup(content, "html.parser")
    for h2 in soup.find_all("h2"):
        txt = h2.text.strip()
        if txt.startswith("CCGS"):
            kwargs["platform_name"] = txt[4:].strip()
            kwargs["prefix"] = "CCGS"
            kwargs["country"] = "CA"
    for line in _clean_block(soup.find("label", {"for": "vessel.officialNumber"})):
        if line.startswith("Official Number:"):
            kwargs["registry_ca"] = line[16:].strip() or None
        elif line.startswith("Call Sign:"):
            kwargs["platform_id"] = line[10:].strip() or None
    nxt = False
    for line in _clean_block(soup.find("label", {"for": "vesselCertificate.imo"})):
        if line.startswith("IMO:"):
            nxt = True
        elif nxt:
            kwargs["imo"] = _clean_na(line.strip())
            nxt = False
    nxt = False
    for line in _clean_block(soup.find("label", {"for": "vesselConstruction.yearInService"})):
        if line.startswith("Year in Service:"):
            nxt = True
        elif nxt:
            kwargs["start_date"] = _clean_na(line.strip())
    nxt = False
    for line in _clean_block(soup.find("label", {"for": "vesselConstruction.yearDisposed"})):
        if line.startswith("Year Disposed:"):
            nxt = True
        elif nxt:
            kwargs["end_date"] = _clean_na(line.strip())
    nxt = False
    for line in _clean_block(soup.find("label", {"for": "vesselSpecification.speedMax"})):
        if line.startswith("Maximum Speed (kts):"):
            nxt = True
        elif nxt:
            kwargs["max_speed"] = _clean_na(line.strip(), " kts")
    return PlatformInfo(**kwargs, platform_type="ship")

def _clean_na(txt: str, suffix: str = "") -> str | None:
    if txt.lower() == "not applicable":
        return None
    elif txt.lower() == "no":
        return None
    elif not txt:
        return None
    else:
        return f"{txt}{suffix}"

def _clean_block(block: BeautifulSoup | None) -> list[str]:
    if not block:
        return []
    txt = block.parent.text.replace("\r", "\n")
    while "\n\n" in txt:
        txt = txt.replace("\n\n", "\n")
    while "  " in txt:
        txt = txt.replace("  ", " ")
    txt = txt.strip()
    return [
        x.strip()
        for x in txt.split("\n")
        if x.strip()
    ]
