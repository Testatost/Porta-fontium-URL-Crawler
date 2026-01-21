import os
import re
import json
import time
import webbrowser
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin, urlparse, urlencode, parse_qsl, urlunparse

import requests
from bs4 import BeautifulSoup
import urllib3
import warnings

from PySide6.QtCore import QObject, QThread, Signal, Qt
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QFileDialog, QMessageBox, QTextEdit,
    QTabWidget, QScrollArea, QGroupBox, QComboBox, QRadioButton, QCheckBox,
    QButtonGroup, QSizePolicy, QSpacerItem
)

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
BASE = "https://www.portafontium.eu"
UA = {"User-Agent": "pf-pyside6-multitab-crawler/4.5 (personal use)"}

PF_VERIFY = False  # bei dir: PortaFontium TLS problematisch
if PF_VERIFY is False:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    s.verify = PF_VERIFY
    return s


def fetch_html(session: requests.Session, url: str, timeout=30) -> str:
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text


# ------------------------------------------------------------
# Sprache
# ------------------------------------------------------------
STR = {
    "de": {
        "app_title": "PortaFontium – Link-Crawler",
        "lang_label": "Sprache:",
        "save_folder": "Speicherordner:",
        "choose_folder": "Ordner wählen…",
        "start": "Crawl starten",
        "stop": "Stopp",
        "ready": "Bereit.",
        "running": "Crawl läuft…",
        "stopping": "Stop angefordert…",
        "saved": "✅ Gespeichert: {n} Einträge → {path}",
        "err_folder": "Bitte einen Speicherordner auswählen.",
        "err_form": "Formular konnte nicht geladen werden.",
        "err": "Fehler: {msg}",
        "home": "Zur Webseite – {tab}",
        "crawler_opts": "Crawler",
        "delay": "Delay (s):",
        "max_pages": "Max Seiten:",
        "tab_register": "Matriken",
        "tab_chronicle": "Chroniken",
        "tab_charter": "Urkunden",
        "tab_photo": "Fotos",
        "tab_census": "Volkszählung",
        "tab_map": "Karten",
        "tab_periodical": "Zeitungen",
        "tab_amtsbuch": "Amtsbücher",
        "log_prefix": "[{tab}] ",
    },
    "cs": {
        "app_title": "PortaFontium – Link-Crawler",
        "lang_label": "Jazyk:",
        "save_folder": "Složka pro uložení:",
        "choose_folder": "Vybrat…",
        "start": "Spustit crawl",
        "stop": "Stop",
        "ready": "Připraveno.",
        "running": "Crawl běží…",
        "stopping": "Stop požadován…",
        "saved": "✅ Uloženo: {n} položek → {path}",
        "err_folder": "Vyber složku pro uložení.",
        "err_form": "Formulář nelze načíst.",
        "err": "Chyba: {msg}",
        "home": "Na web – {tab}",
        "crawler_opts": "Crawler",
        "delay": "Delay (s):",
        "max_pages": "Max stránek:",
        "tab_register": "Matriky",
        "tab_chronicle": "Kroniky",
        "tab_charter": "Listiny",
        "tab_photo": "Fotografie",
        "tab_census": "Sčítání",
        "tab_map": "Mapy",
        "tab_periodical": "Periodika",
        "tab_amtsbuch": "Úřední knihy",
        "log_prefix": "[{tab}] ",
    }
}


def tr(lang: str, key: str, **kw) -> str:
    s = STR.get(lang, STR["de"]).get(key, key)
    return s.format(**kw) if kw else s


# ------------------------------------------------------------
# Tab-Definitionen
# ------------------------------------------------------------
@dataclass
class TabDef:
    key: str
    path: str
    allowed_prefixes: Tuple[str, ...]
    prefer_iipimage_root: bool = True
    prefer_iipimage_only_if_present: bool = True


TABS: List[TabDef] = [
    TabDef("register",   "/searching/register",   ("/register/", "/iipimage/")),
    TabDef("chronicle",  "/searching/chronicle",  ("/chronicle/", "/iipimage/")),
    TabDef("charter",    "/searching/charter",    ("/charter/", "/iipimage/")),
    TabDef("photo",      "/searching/photo",      ("/photo/", "/iipimage/")),
    TabDef("census",     "/searching/census",     ("/census/", "/iipimage/")),
    TabDef("map",        "/searching/map",        ("/map/", "/iipimage/")),
    # Zeitungen: NICHT iipimage-only, weil wir /periodical/ brauchen
    TabDef("periodical", "/searching/periodical", ("/iipimage/", "/periodical/"), prefer_iipimage_only_if_present=False),
    TabDef("amtsbuch",   "/searching/amtsbuch",   ("/amtsbuch/", "/iipimage/")),
]


# ------------------------------------------------------------
# URL helpers
# ------------------------------------------------------------
def strip_language_param(url: str) -> str:
    if not url:
        return url
    p = urlparse(url)
    if not p.query:
        return urlunparse((p.scheme, p.netloc, p.path, p.params, "", ""))
    qs = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k.lower() != "language"]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, ""))


# ------------------------------------------------------------
# Drupal View Info parsing
# ------------------------------------------------------------
def parse_drupal_view_info(html: str) -> dict:
    info = {}

    m_name = re.search(r"\bview-id-([a-zA-Z0-9_]+)\b", html)
    m_disp = re.search(r"\bview-display-id-([a-zA-Z0-9_]+)\b", html)
    m_dom = re.search(r"\bview-dom-id-([a-f0-9]{32})\b", html)
    if m_name:
        info["view_name"] = m_name.group(1)
    if m_disp:
        info["view_display_id"] = m_disp.group(1)
    if m_dom:
        info["view_dom_id"] = m_dom.group(1)

    m_form = re.search(r'id="views-exposed-form-([a-zA-Z0-9_]+)-([a-zA-Z0-9_\-]+)"', html)
    if m_form:
        info.setdefault("view_name", m_form.group(1))
        info.setdefault("view_display_id", m_form.group(2).replace("-", "_"))

    m_theme = re.search(r'"theme"\s*:\s*"([^"]+)"', html)
    m_token = re.search(r'"theme_token"\s*:\s*"([^"]+)"', html)
    if m_theme:
        info["theme"] = m_theme.group(1)
    if m_token:
        info["theme_token"] = m_token.group(1)

    return info


# ------------------------------------------------------------
# Formular Parsing (robuster – wichtig für "Karten")
# ------------------------------------------------------------
@dataclass
class WidgetSpec:
    kind: str  # "radio" | "checkbox" | "select" | "text"
    name: str
    label: str
    options: List[Tuple[str, str]]
    default: Optional[str] = None
    defaults_multi: Optional[List[str]] = None


def _clean_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def pick_default_option(opts: List[Tuple[str, str]]) -> str:
    if not opts:
        return ""
    # 1) value="" bevorzugen
    for lab, val in opts:
        if (val or "") == "":
            return val
    # 2) value "All"
    for lab, val in opts:
        if (val or "").lower() == "all":
            return val
    # 3) Label enthält "Alle" / "Vše" / "All"
    for lab, val in opts:
        low = (lab or "").strip().lower()
        if "alle" in low or "vše" in low or low == "all":
            return val
    return opts[0][1]


# ------------------------------------------------------------
# PF Label/Option Übersetzung (DE) + Fallback anhand Feldnamen
# ------------------------------------------------------------
PF_TEXT_DE = {
    # häufige Feld-Labels (CZ -> DE)
    "Místo": "Ort",
    "Misto": "Ort",
    "Od roku": "Von Jahr",
    "Do roku": "Bis Jahr",
    "Von Jahr": "Von Jahr",
    "Bis Jahr": "Bis Jahr",
    "Seit Jahr": "Von Jahr",
    "Bis Jahr": "Bis Jahr",
    "Nadpis": "Titel",
    "Titulek": "Titel",
    "Text": "Text",
    "Archiv": "Archiv",
    "Typ": "Typ",
    "Jazyk": "Sprache",
    "Kroniky": "Chroniken",
    "Signatur": "Signatur",
    "Verlagsort": "Erscheinungsort",

    # View-Mode (Karten)
    "v seznamu": "In Liste",
    "na mapě": "Auf Karte",
}

PF_OPTION_DE = {
    # Chroniken -> Typ (CZ -> DE)
    "Obecní kronika": "Gemeindechronik",
    "Školní kronika": "Schulchronik",
    "Cirkevní kronika": "Kirchenchronik",
    "Církevní kronika": "Kirchenchronik",
    "Jiné kroniky": "Andere Chroniken",
    "Fotografie": "Fotografien",

    # Chroniken -> Radio
    "Všechny kroniky": "Alle Chroniken",
    "Pouze s obrázky": "Nur mit Bildern",

    # Zeitungen -> Typ
    "Periodikum": "Periodikum",
    "Kur_listce, Adressbuch": "Kurliste / Adressbuch",
    "Kur_listce, adresář": "Kurliste / Adressbuch",
    "Kurliste, Adressbuch": "Kurliste / Adressbuch",

    # Sprache
    "CZ": "Tschechisch (CZ)",
    "DE": "Deutsch (DE)",

    # allgemeine "Alle"
    "- Alle -": "- Alle -",
    "- Vše -": "- Alle -",
}


def _pf_translate_de(text: str) -> str:
    """Übersetzt bekannte PF-Texte (CZ/DE gemischt) in sinnvolle DE-Begriffe."""
    t = _clean_label(text or "")
    if not t:
        return t
    # zuerst harte Treffer
    if t in PF_TEXT_DE:
        return PF_TEXT_DE[t]
    if t in PF_OPTION_DE:
        return PF_OPTION_DE[t]
    return t


def _pf_label_from_name_de(name: str, current_label: str) -> str:
    """
    Fallback: wenn PF kein gutes Label liefert (oder technisches),
    erzeugen wir ein sinnvolles DE-Label anhand des Feldnamens.
    """
    n = (name or "").lower()
    cur = _clean_label(current_label or "")
    # Umkreis-Felder immer erzwingen (PF nutzt meist okoli/okolí)
    if any(k in n for k in ["okoli", "okolí", "vicinity", "radius", "distance", "umkreis"]):
        return "Umkreis"


    # wenn Label fehlt oder 1:1 der Feldname ist -> heuristik
    if (not cur) or (cur.lower() == n):
        if any(k in n for k in ["archiv", "archive"]):
            return "Archiv"
        if any(k in n for k in ["title", "titel", "nadpis"]):
            return "Titel"
        if any(k in n for k in ["misto", "místo", "place", "ort", "lokal", "location"]):
            return "Ort"
        if any(k in n for k in ["text", "fulltext", "query", "q"]):
            return "Text"
        if any(k in n for k in ["od_roku", "from", "seit", "von", "_from"]):
            return "Von Jahr"
        if any(k in n for k in ["do_roku", "to", "bis", "_to"]):
            return "Bis Jahr"
        if any(k in n for k in ["typ", "type"]):
            return "Typ"
        if any(k in n for k in ["jazyk", "language", "sprache"]):
            return "Sprache"
        if any(k in n for k in ["signatur", "signature"]):
            return "Signatur"
        if any(k in n for k in ["verlagsort", "publisher", "place_of_pub"]):
            return "Erscheinungsort"
        if any(k in n for k in ["kronik", "chronicle"]):
            return "Chroniken"

    return cur or name


def localize_widget_for_lang(lang: str, ws: "WidgetSpec") -> "WidgetSpec":
    """Wendet DE-Übersetzung/Normalisierung auf Widget + Optionen an."""
    if lang != "de":
        return ws

    # 1) Gruppenlabel übersetzen + fallback nach name
    ws.label = _pf_translate_de(ws.label)
    ws.label = _pf_label_from_name_de(ws.name, ws.label)

    # 2) Optionen übersetzen (Radio/Checkbox/Select)
    if ws.options:
        new_opts = []
        for lab, val in ws.options:
            lab2 = _pf_translate_de(lab)
            # falls PF_OPTION_DE nicht greift, wenigstens CZ Feldlabels abfangen
            lab2 = PF_OPTION_DE.get(_clean_label(lab), lab2)
            new_opts.append((lab2, val))
        ws.options = new_opts

    return ws



_INPUT_TEXT_TYPES = {"text", "search", "number", "tel", "email", "url"}


def _wrapper_label(form: BeautifulSoup, wrapper) -> str:
    # 1) label im Wrapper (nur "echtes" Label)
    lab = ""
    lt = wrapper.find("label")
    if lt:
        lab = _clean_label(lt.get_text(" ", strip=True))
    # 2) fallback: legend
    if not lab:
        lg = wrapper.find("legend")
        if lg:
            lab = _clean_label(lg.get_text(" ", strip=True))
    # 3) fallback: aria-label / placeholder von erstem input
    if not lab:
        first = wrapper.find(["input", "select", "textarea"])
        if first:
            lab = _clean_label(first.get("aria-label") or first.get("placeholder") or "")
    return lab


def _element_label(form, elem) -> str:
    # label[for=id] bevorzugen
    eid = elem.get("id")
    if eid:
        l = form.find("label", attrs={"for": eid})
        if l:
            return _clean_label(l.get_text(" ", strip=True))
    # fallback: parent text
    parent = elem.parent
    if parent:
        txt = _clean_label(parent.get_text(" ", strip=True))
        return txt
    return ""


def _is_ignored_input(inp) -> bool:
    t = (inp.get("type") or "").lower()
    if t in {"submit", "button", "image", "hidden", "reset"}:
        return True
    if inp.has_attr("disabled"):
        return True
    return False


def _is_ignored_control(tag) -> bool:
    if tag.name == "input":
        return _is_ignored_input(tag)
    if tag.name in {"select", "textarea"}:
        return tag.has_attr("disabled")
    return True


def load_form_spec(session: requests.Session, tab: TabDef, lang: str) -> dict:
    url = f"{BASE}{tab.path}?language={lang}"
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "lxml")

    form = soup.find("form", id=re.compile(r"^views-exposed-form-"))
    if not form:
        raise RuntimeError("Kein Views-Formular gefunden.")

    action = urljoin(BASE, form.get("action") or tab.path)
    view_info = parse_drupal_view_info(html)

    widgets: List[WidgetSpec] = []
    seen_names = set()

    wrappers = list(form.select(".views-exposed-widget"))
    # Fallback + Ergänzung: bei Karten & Co sind manchmal relevante Felder nur in .form-item
    # Wir nehmen zusätzlich .form-item, die NICHT in einem views-exposed-widget liegen.
    extra = []
    for it in form.select(".form-item"):
        if it.find_parent(class_="views-exposed-widget") is None:
            extra.append(it)
    wrappers.extend(extra)

    # Wenn gar keine Wrapper gefunden: notfalls ganze Form als Wrapper
    if not wrappers:
        wrappers = [form]

    def add_radio_group(name: str, group_label: str, inputs):
        if not name or name in seen_names:
            return
        opts = []
        default = None
        for inp in inputs:
            val = inp.get("value") or ""
            lab = _element_label(form, inp) or val
            opts.append((_clean_label(lab), val))
            if inp.has_attr("checked"):
                default = val
        if default is None:
            default = pick_default_option(opts)

        widgets.append(WidgetSpec(kind="radio", name=name, label=group_label or name, options=opts, default=default))
        seen_names.add(name)

    def add_checkbox_group(name: str, group_label: str, inputs):
        if not name or name in seen_names:
            return
        opts = []
        defaults = []
        for inp in inputs:
            val = inp.get("value") or "1"
            lab = _element_label(form, inp) or name
            opts.append((_clean_label(lab), val))
            if inp.has_attr("checked"):
                defaults.append(val)
        widgets.append(WidgetSpec(kind="checkbox", name=name, label=group_label or name, options=opts, defaults_multi=defaults))
        seen_names.add(name)

    def add_select(sel, label: str):
        name = sel.get("name") or ""
        if not name or name in seen_names:
            return
        opts = []
        default = None
        for opt in sel.select("option"):
            lab = _clean_label(opt.get_text(" ", strip=True))
            val = opt.get("value") or ""
            opts.append((lab, val))
            if opt.has_attr("selected"):
                default = val
        if default is None:
            default = pick_default_option(opts)

        widgets.append(WidgetSpec(kind="select", name=name, label=label or name, options=opts, default=default))
        seen_names.add(name)

    def add_text(inp, label: str):
        name = inp.get("name") or ""
        if not name or name in seen_names:
            return
        default = inp.get("value") or ""
        widgets.append(WidgetSpec(kind="text", name=name, label=label or name, options=[], default=default))
        seen_names.add(name)

    # 1) Wrapper-basiert (bevorzugt)
    for w in wrappers:
        # submit block skippen
        if w.select_one("input[type=submit], button[type=submit]"):
            continue

        wlabel = _wrapper_label(form, w)

        # Wenn ein Wrapper mehrere unterschiedliche Controls enthält,
        # darf wlabel NICHT für alle genommen werden (sonst wird z.B. Umkreis = Ort).
        names_in_wrapper = []
        for t in w.select("input[name], select[name], textarea[name]"):
            if _is_ignored_control(t):
                continue
            nm = t.get("name") or ""
            if nm:
                names_in_wrapper.append(nm)
        wrapper_has_multiple = len(set(names_in_wrapper)) > 1


        # gruppiere radios/checkboxes nach name
        radios_by_name: Dict[str, List] = {}
        checks_by_name: Dict[str, List] = {}

        for inp in w.select("input[name]"):
            if _is_ignored_input(inp):
                continue
            t = (inp.get("type") or "").lower()

            if t == "radio":
                radios_by_name.setdefault(inp.get("name") or "", []).append(inp)
            elif t == "checkbox":
                checks_by_name.setdefault(inp.get("name") or "", []).append(inp)
            elif t in _INPUT_TEXT_TYPES:
                # Textfeld
                lbl = _element_label(form, inp)
                if not lbl and not wrapper_has_multiple:
                    lbl = wlabel
                add_text(inp, lbl)


        for name, inputs in radios_by_name.items():
            add_radio_group(name, wlabel, inputs)
        for name, inputs in checks_by_name.items():
            add_checkbox_group(name, wlabel, inputs)

        for sel in w.select("select[name]"):
            if _is_ignored_control(sel):
                continue
            lbl = _element_label(form, sel)
            if not lbl and not wrapper_has_multiple:
                lbl = wlabel
            add_select(sel, lbl)

        for ta in w.select("textarea[name]"):
            if _is_ignored_control(ta):
                continue
            # Textarea als "text" behandeln
            lbl = _element_label(form, ta)
            if not lbl and not wrapper_has_multiple:
                lbl = wlabel
            add_text(ta, lbl)


    # 2) Finale Ergänzung: alle remaining named controls (falls Karten-Feld komplett außerhalb der Wrapper liegt)
    for tag in form.select("input[name], select[name], textarea[name]"):
        if _is_ignored_control(tag):
            continue
        name = tag.get("name") or ""
        if not name or name in seen_names:
            continue

        if tag.name == "select":
            add_select(tag, _element_label(form, tag))
        elif tag.name == "textarea":
            add_text(tag, _element_label(form, tag))
        elif tag.name == "input":
            t = (tag.get("type") or "").lower()
            if t == "radio":
                # komplette radio-gruppe suchen
                grp = form.select(f'input[type="radio"][name="{name}"]')
                add_radio_group(name, _element_label(form, tag), grp)
            elif t == "checkbox":
                grp = form.select(f'input[type="checkbox"][name="{name}"]')
                add_checkbox_group(name, _element_label(form, tag), grp)
            elif t in _INPUT_TEXT_TYPES:
                add_text(tag, _element_label(form, tag))

    # ------------------------------------------------------------
    # Lokalisierung (DE): Labels + Optionen "PF-like" übersetzen
    # ------------------------------------------------------------
    widgets = [localize_widget_for_lang(lang, w) for w in widgets]

    return {"action": action, "view_info": view_info, "widgets": widgets, "raw_html": html}



# ------------------------------------------------------------
# Link extraction
# ------------------------------------------------------------
def extract_iipimage_ids_anywhere(text: str) -> List[str]:
    ids = re.findall(r"/iipimage/(\d+)", text or "")
    seen = set()
    out = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def normalize_pf_link(tab: TabDef, href: str) -> Optional[str]:
    if not href:
        return None
    full = urljoin(BASE, href)
    p = urlparse(full)
    if not p.netloc.endswith("portafontium.eu"):
        return None

    path = p.path or ""
    if not any(path.startswith(pref) for pref in tab.allowed_prefixes):
        return None

    if tab.prefer_iipimage_root and path.startswith("/iipimage/"):
        m = re.match(r"^/iipimage/(\d+)", path)
        if m:
            return f"{BASE}/iipimage/{m.group(1)}"

    return strip_language_param(p._replace(fragment="").geturl())


def extract_links_from_html(tab: TabDef, html: str) -> List[str]:
    if tab.key != "periodical":
        ids = extract_iipimage_ids_anywhere(html)
        if ids and tab.prefer_iipimage_only_if_present:
            out = []
            seen = set()
            for pid in ids:
                u = f"{BASE}/iipimage/{pid}"
                if u not in seen:
                    seen.add(u)
                    out.append(u)
            return out

    out = []
    seen = set()
    try:
        soup = BeautifulSoup(html or "", "lxml")
        scope = (
            soup.select_one("table.views-table") or
            soup.select_one(".view-content") or
            soup.select_one(".view") or
            soup
        )
        for a in scope.select("a[href]"):
            u = normalize_pf_link(tab, a.get("href"))
            if not u:
                continue
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
    except Exception:
        pass

    if tab.key == "periodical":
        ids = extract_iipimage_ids_anywhere(html)
        for pid in ids:
            u = strip_language_param(f"{BASE}/iipimage/{pid}")
            if u not in seen:
                seen.add(u)
                out.append(u)

    return out


# ------------------------------------------------------------
# Views AJAX
# ------------------------------------------------------------
def drupal_views_ajax_fetch(
    session: requests.Session,
    view_name: str,
    view_display_id: str,
    view_dom_id: str,
    view_path: str,
    page: int,
    exposed_items: List[Tuple[str, str]],
    theme: Optional[str],
    theme_token: Optional[str],
    referer_lang: str,
) -> str:
    ajax_url = f"{BASE}/views/ajax"

    base_items = [
        ("view_name", view_name),
        ("view_display_id", view_display_id),
        ("view_args", ""),
        ("view_path", view_path),
        ("view_base_path", view_path),
        ("view_dom_id", view_dom_id),
        ("pager_element", "0"),
        ("page", str(page)),
    ]
    if theme:
        base_items.append(("ajax_page_state[theme]", theme))
    if theme_token:
        base_items.append(("ajax_page_state[theme_token]", theme_token))

    data_items = base_items + [(k, "" if v is None else str(v)) for (k, v) in exposed_items]

    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": f"{BASE}/{view_path}?language={referer_lang}",
    }

    r = session.post(ajax_url, data=data_items, headers=headers, timeout=30)
    r.raise_for_status()

    try:
        cmds = r.json()
    except Exception:
        return r.text

    html_parts = []
    if isinstance(cmds, list):
        for cmd in cmds:
            if isinstance(cmd, dict):
                for k in ("data", "markup"):
                    v = cmd.get(k)
                    if isinstance(v, str) and "<" in v:
                        html_parts.append(v)
    return "\n".join(html_parts)


def build_visible_url(action_url: str, lang: str, exposed_items: List[Tuple[str, str]], page: int) -> str:
    items = list(exposed_items)
    if not any(k == "language" for k, _ in items):
        items.append(("language", lang))
    items.append(("page", str(page)))
    qs = urlencode(items, doseq=True)
    sep = "&" if "?" in action_url else "?"
    return action_url + sep + qs


# ------------------------------------------------------------
# Periodika: über /periodical/ Seiten alle Ausgaben holen
# ------------------------------------------------------------
def expand_periodicals_via_periodical_pages(
    session: requests.Session,
    links: List[str],
    delay_s: float,
    log_cb=None,
    stop_flag=lambda: False,
) -> List[str]:
    periodical_pages = [u for u in links if "/periodical/" in u]
    if not periodical_pages:
        return links

    out_set = set(links)
    out_list = list(links)

    if log_cb:
        log_cb(f"[Periodika] Öffne {len(periodical_pages)} /periodical/-Seiten und lese alle Ausgaben …")

    for idx, purl in enumerate(periodical_pages, start=1):
        if stop_flag():
            break
        try:
            html = fetch_html(session, purl, timeout=30)
        except Exception as e:
            if log_cb:
                log_cb(f"  [Warn] /periodical/ Seite nicht ladbar ({idx}/{len(periodical_pages)}): {purl} -> {e}")
            continue

        ids = extract_iipimage_ids_anywhere(html)
        new_cnt = 0
        for pid in ids:
            u = strip_language_param(f"{BASE}/iipimage/{pid}")
            if u not in out_set:
                out_set.add(u)
                out_list.append(u)
                new_cnt += 1

        if log_cb and (idx == 1 or idx % 10 == 0 or idx == len(periodical_pages)):
            log_cb(f"  {idx}/{len(periodical_pages)}: +{new_cnt} Ausgaben (gesamt {len(out_list)})")

        if delay_s > 0:
            time.sleep(delay_s)

    # am Ende nur Ausgaben (iipimage)
    final = []
    seen = set()
    for u in out_list:
        u = strip_language_param(u)
        if "/iipimage/" not in u:
            continue
        if u not in seen:
            seen.add(u)
            final.append(u)
    return final


# ------------------------------------------------------------
# Crawl
# ------------------------------------------------------------
def crawl_tab_links(
    tab: TabDef,
    lang: str,
    action_url: str,
    exposed_items: List[Tuple[str, str]],
    view_info: dict,
    max_pages: int,
    delay_s: float,
    log_cb=None,
    stop_flag=lambda: False,
) -> List[str]:
    session = make_session()

    view_name = view_info.get("view_name") or "solr_searching"
    view_display_id = view_info.get("view_display_id") or ""
    view_dom_id = view_info.get("view_dom_id")
    theme = view_info.get("theme")
    theme_token = view_info.get("theme_token")
    view_path = tab.path.lstrip("/")

    if not view_dom_id or not view_display_id:
        boot = fetch_html(session, f"{BASE}{tab.path}?language={lang}")
        boot_info = parse_drupal_view_info(boot)
        view_dom_id = view_dom_id or boot_info.get("view_dom_id")
        view_display_id = view_display_id or boot_info.get("view_display_id") or ""
        view_name = view_name or boot_info.get("view_name") or "solr_searching"
        theme = theme or boot_info.get("theme")
        theme_token = theme_token or boot_info.get("theme_token")

    if not view_dom_id:
        raise RuntimeError("Konnte view_dom_id nicht ermitteln.")
    if not view_display_id:
        raise RuntimeError("Konnte view_display_id nicht ermitteln.")

    if log_cb:
        log_cb(f"[Debug] view_name={view_name} view_display_id={view_display_id} view_dom_id={view_dom_id}")

    all_links: List[str] = []
    seen = set()
    empty_streak = 0
    no_new_streak = 0

    for page in range(max_pages):
        if stop_flag():
            break

        visible_url = build_visible_url(action_url, lang, exposed_items, page)
        if log_cb:
            log_cb(f"[Suche] page={page} GET -> {visible_url}")

        page_links: List[str] = []
        try:
            html = fetch_html(session, visible_url)
            page_links = extract_links_from_html(tab, html)
        except Exception as e:
            if log_cb:
                log_cb(f"  [Warn] GET fehlgeschlagen: {e} (versuche AJAX)")

        if not page_links:
            if log_cb:
                log_cb("  [Info] Keine Treffer im GET – nutze /views/ajax …")

            frag = drupal_views_ajax_fetch(
                session=session,
                view_name=view_name,
                view_display_id=view_display_id,
                view_dom_id=view_dom_id,
                view_path=view_path,
                page=page,
                exposed_items=exposed_items,
                theme=theme,
                theme_token=theme_token,
                referer_lang=lang,
            )
            page_links = extract_links_from_html(tab, frag)

        if not page_links:
            empty_streak += 1
            if log_cb:
                log_cb(f"  (keine Treffer) streak={empty_streak}")
            if empty_streak >= 2:
                break
            continue
        empty_streak = 0

        new_cnt = 0
        for u in page_links:
            u = strip_language_param(u)
            if u in seen:
                continue
            seen.add(u)
            all_links.append(u)
            new_cnt += 1

        if log_cb:
            log_cb(f"  Treffer: {len(page_links)} | neu: {new_cnt} | gesamt: {len(all_links)}")

        if new_cnt == 0:
            no_new_streak += 1
            if log_cb:
                log_cb(f"  (keine neuen Links) streak={no_new_streak}")
            if no_new_streak >= 2:
                break
        else:
            no_new_streak = 0

        if delay_s > 0:
            time.sleep(delay_s)

    if tab.key == "periodical":
        all_links = expand_periodicals_via_periodical_pages(
            session=session,
            links=all_links,
            delay_s=delay_s,
            log_cb=log_cb,
            stop_flag=stop_flag,
        )

    final = []
    final_seen = set()
    for u in all_links:
        u = strip_language_param(u)
        if u not in final_seen:
            final_seen.add(u)
            final.append(u)

    return final


# ------------------------------------------------------------
# Worker Thread (PySide6)
# ------------------------------------------------------------
class CrawlWorker(QObject):
    log = Signal(str)
    done = Signal(list, str)
    failed = Signal(str)

    def __init__(
        self,
        tab: TabDef,
        lang: str,
        action_url: str,
        exposed_items: List[Tuple[str, str]],
        view_info: dict,
        max_pages: int,
        delay_s: float,
        json_path: str,
        outdir: str,
        stop_flag_callable,
    ):
        super().__init__()
        self.tab = tab
        self.lang = lang
        self.action_url = action_url
        self.exposed_items = exposed_items
        self.view_info = view_info
        self.max_pages = max_pages
        self.delay_s = delay_s
        self.json_path = json_path
        self.outdir = outdir
        self.stop_flag_callable = stop_flag_callable

    def run(self):
        try:
            links = crawl_tab_links(
                tab=self.tab,
                lang=self.lang,
                action_url=self.action_url,
                exposed_items=self.exposed_items,
                view_info=self.view_info,
                max_pages=self.max_pages,
                delay_s=self.delay_s,
                log_cb=lambda m: self.log.emit(m),
                stop_flag=self.stop_flag_callable,
            )

            items = [{"url": strip_language_param(u), "outdir": self.outdir, "pages": ""} for u in links]
            os.makedirs(os.path.dirname(self.json_path), exist_ok=True)
            with open(self.json_path, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)

            self.done.emit(links, self.json_path)
        except Exception as e:
            self.failed.emit(str(e))


# ------------------------------------------------------------
# MainWindow (PySide6 UI)
# ------------------------------------------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.lang = "de"
        self.session = make_session()

        self.setWindowTitle(tr(self.lang, "app_title"))
        self.resize(1200, 820)

        self.forms: Dict[str, dict] = {}
        self.tab_ui: Dict[str, dict] = {}

        self._stop = False
        self._thread: Optional[QThread] = None
        self._worker: Optional[CrawlWorker] = None

        self._build_ui()
        self._load_all_forms()

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        main = QVBoxLayout(root)
        main.setContentsMargins(10, 10, 10, 10)
        main.setSpacing(8)

        top = QHBoxLayout()
        top.setSpacing(8)
        main.addLayout(top)

        top.addWidget(QLabel(tr(self.lang, "lang_label")))
        self.btn_lang_de = QPushButton("DE")
        self.btn_lang_cs = QPushButton("CZ")
        self.btn_lang_de.setCheckable(True)
        self.btn_lang_cs.setCheckable(True)
        self.btn_lang_de.setChecked(True)
        self.btn_lang_de.clicked.connect(lambda: self._set_lang("de"))
        self.btn_lang_cs.clicked.connect(lambda: self._set_lang("cs"))
        top.addWidget(self.btn_lang_de)
        top.addWidget(self.btn_lang_cs)
        top.addSpacing(16)

        top.addWidget(QLabel(tr(self.lang, "save_folder")))
        self.ed_folder = QLineEdit()
        self.ed_folder.setPlaceholderText(tr(self.lang, "save_folder"))
        top.addWidget(self.ed_folder, 1)
        self.btn_folder = QPushButton(tr(self.lang, "choose_folder"))
        self.btn_folder.clicked.connect(self._choose_folder)
        top.addWidget(self.btn_folder)

        self.tabs = QTabWidget()
        main.addWidget(self.tabs, 1)

        for tab in TABS:
            tabw = QWidget()
            self.tabs.addTab(tabw, self._tab_title(tab.key))
            self._build_tab(tab, tabw)

        bottom_btns = QHBoxLayout()
        bottom_btns.setSpacing(8)
        main.addLayout(bottom_btns)

        self.btn_start = QPushButton(tr(self.lang, "start"))
        self.btn_stop = QPushButton(tr(self.lang, "stop"))
        self.btn_stop.setEnabled(False)

        self.btn_start.clicked.connect(self._start_crawl_current_tab)
        self.btn_stop.clicked.connect(self._stop_crawl)

        bottom_btns.addWidget(self.btn_start)
        bottom_btns.addWidget(self.btn_stop)
        bottom_btns.addSpacing(10)
        self.lbl_status = QLabel(tr(self.lang, "ready"))
        bottom_btns.addWidget(self.lbl_status, 1)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setMinimumHeight(230)
        main.addWidget(self.log)
        self.log.document().setMaximumBlockCount(4000)  # begrenzt Logzeilen (RAM-Schutz)

    def _choose_folder(self):
        d = QFileDialog.getExistingDirectory(self, tr(self.lang, "choose_folder"))
        if d:
            self.ed_folder.setText(d)

    def _set_lang(self, lang: str):
        self.lang = lang
        self.btn_lang_de.setChecked(lang == "de")
        self.btn_lang_cs.setChecked(lang == "cs")
        self.setWindowTitle(tr(self.lang, "app_title"))

        self.btn_folder.setText(tr(self.lang, "choose_folder"))
        self.btn_start.setText(tr(self.lang, "start"))
        self.btn_stop.setText(tr(self.lang, "stop"))
        self.lbl_status.setText(tr(self.lang, "ready"))

        for i, tab in enumerate(TABS):
            self.tabs.setTabText(i, self._tab_title(tab.key))
        self._load_all_forms()

    def _tab_title(self, key: str) -> str:
        return tr(self.lang, f"tab_{key}")

    def _append_log(self, msg: str):
        self.log.append(msg)
        # Auto-Scroll ans Ende
        cursor = self.log.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log.setTextCursor(cursor)
        self.log.ensureCursorVisible()

    def _build_tab(self, tab: TabDef, parent: QWidget):
        outer = QVBoxLayout(parent)
        outer.setContentsMargins(8, 8, 8, 8)
        outer.setSpacing(6)

        row = QHBoxLayout()
        row.setSpacing(6)
        btn_home = QPushButton(tr(self.lang, "home", tab=self._tab_title(tab.key)))
        btn_home.clicked.connect(lambda: webbrowser.open(f"{BASE}{tab.path}?language={self.lang}"))
        btn_home.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
        row.addWidget(btn_home)
        row.addItem(QSpacerItem(10, 10, QSizePolicy.Expanding, QSizePolicy.Minimum))
        outer.addLayout(row)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        outer.addWidget(scroll, 1)

        content = QWidget()
        scroll.setWidget(content)

        content_v = QVBoxLayout(content)
        content_v.setContentsMargins(0, 0, 0, 0)
        content_v.setSpacing(6)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(8)
        grid.setVerticalSpacing(8)

        content_v.addLayout(grid)
        content_v.addStretch(1)

        self.tab_ui[tab.key] = {
            "tab": tab,
            "scroll": scroll,
            "content": content,
            "grid": grid,
            "controls": {},
            "home_btn": btn_home,
        }

    def _load_all_forms(self):
        self.forms.clear()
        for tab in TABS:
            self._load_form_for_tab(tab)

    def _clear_layout(self, layout):
        while layout.count():
            item = layout.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()
            elif item.layout() is not None:
                self._clear_layout(item.layout())

    def _load_form_for_tab(self, tab: TabDef):
        ui = self.tab_ui[tab.key]
        grid: QGridLayout = ui["grid"]
        self._clear_layout(grid)
        ui["controls"] = {}

        ui["home_btn"].setText(tr(self.lang, "home", tab=self._tab_title(tab.key)))

        try:
            spec = load_form_spec(self.session, tab, self.lang)
            self.forms[tab.key] = spec
        except Exception as e:
            box = QGroupBox(tr(self.lang, "err_form"))
            v = QVBoxLayout(box)
            v.setContentsMargins(8, 8, 8, 8)
            v.addWidget(QLabel(str(e)))
            grid.addWidget(box, 0, 0, 1, 1)
            return

        widgets: List[WidgetSpec] = spec["widgets"]

        col_count = 3
        r = 0
        c = 0

        def groupbox(title: str) -> QGroupBox:
            g = QGroupBox(title)
            g.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Maximum)
            return g

        for ws in widgets:
            g = groupbox(ws.label or ws.name)

            if ws.kind in ("radio", "checkbox"):
                gl = QGridLayout(g)
                gl.setContentsMargins(8, 10, 8, 8)
                gl.setHorizontalSpacing(10)
                gl.setVerticalSpacing(4)
            else:
                gl = QVBoxLayout(g)
                gl.setContentsMargins(8, 10, 8, 8)
                gl.setSpacing(6)

            if ws.kind == "text":
                ed = QLineEdit()
                ed.setText(ws.default or "")
                gl.addWidget(ed)
                ui["controls"][ws.name] = ("text", ed)

            elif ws.kind == "select":
                cmb = QComboBox()
                idx_default = 0
                for i, (lab, val) in enumerate(ws.options):
                    cmb.addItem(lab, val)
                    if ws.default is not None and val == ws.default:
                        idx_default = i
                cmb.setCurrentIndex(idx_default)
                gl.addWidget(cmb)
                ui["controls"][ws.name] = ("select", cmb)

            elif ws.kind == "radio":
                bg = QButtonGroup(self)
                bg.setExclusive(True)
                default_val = ws.default
                opts = ws.options or []
                n = len(opts)
                ncols = 2 if n > 6 else 1
                if n > 14:
                    ncols = 3

                row_i = 0
                col_i = 0
                for lab, val in opts:
                    rb = QRadioButton(lab)
                    rb.setProperty("pf_value", val)
                    bg.addButton(rb)
                    gl.addWidget(rb, row_i, col_i, 1, 1)
                    if default_val is not None and val == default_val:
                        rb.setChecked(True)
                    col_i += 1
                    if col_i >= ncols:
                        col_i = 0
                        row_i += 1

                if not any(b.isChecked() for b in bg.buttons()) and bg.buttons():
                    bg.buttons()[0].setChecked(True)

                ui["controls"][ws.name] = ("radio", bg)

            elif ws.kind == "checkbox":
                cbs = []
                defaults = set(ws.defaults_multi or [])
                opts = ws.options or []
                n = len(opts)
                ncols = 2 if n > 6 else 1
                if n > 14:
                    ncols = 3

                row_i = 0
                col_i = 0
                for lab, val in opts:
                    cb = QCheckBox(lab)
                    cb.setProperty("pf_value", val)
                    if val in defaults:
                        cb.setChecked(True)
                    gl.addWidget(cb, row_i, col_i, 1, 1)
                    cbs.append(cb)
                    col_i += 1
                    if col_i >= ncols:
                        col_i = 0
                        row_i += 1

                ui["controls"][ws.name] = ("checkbox", cbs)

            grid.addWidget(g, r, c, 1, 1, Qt.AlignTop)

            c += 1
            if c >= col_count:
                c = 0
                r += 1

        crawler_box = QGroupBox(tr(self.lang, "crawler_opts"))
        crawler_box.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Maximum)
        hb = QHBoxLayout(crawler_box)
        hb.setContentsMargins(8, 10, 8, 8)
        hb.setSpacing(8)

        hb.addWidget(QLabel(tr(self.lang, "delay")))
        ed_delay = QLineEdit("0.25")
        ed_delay.setMaximumWidth(120)
        hb.addWidget(ed_delay)

        hb.addWidget(QLabel(tr(self.lang, "max_pages")))
        ed_max = QLineEdit("300")
        ed_max.setMaximumWidth(120)
        hb.addWidget(ed_max)

        hb.addItem(QSpacerItem(10, 10, QSizePolicy.Expanding, QSizePolicy.Minimum))

        ui["controls"]["__delay__"] = ("delay", ed_delay)
        ui["controls"]["__maxpages__"] = ("maxpages", ed_max)

        grid.addWidget(crawler_box, r + 1, 0, 1, col_count, Qt.AlignTop)

        for i in range(col_count):
            grid.setColumnStretch(i, 1)

    def _current_tab_key(self) -> str:
        idx = self.tabs.currentIndex()
        return TABS[idx].key

    def _collect_exposed_items(self, tabkey: str) -> Tuple[List[Tuple[str, str]], str, str, str]:
        ui = self.tab_ui[tabkey]
        controls = ui["controls"]

        exposed: List[Tuple[str, str]] = [("language", self.lang)]
        title_label = self._tab_title(tabkey)
        year_from = ""
        year_to = ""

        from_keys = {"from", "od_roku", "seit_jahr", "field_doc_dates_field_doc_dates_from"}
        to_keys = {"to", "do_roku", "bis_jahr", "field_doc_dates_field_doc_dates_to"}

        for name, spec in controls.items():
            if name in ("__delay__", "__maxpages__"):
                continue

            kind = spec[0]

            if kind == "text":
                ed: QLineEdit = spec[1]
                val = (ed.text() or "").strip()
                exposed.append((name, val))
                if name in from_keys and val:
                    year_from = val
                if name in to_keys and val:
                    year_to = val

            elif kind == "select":
                cmb: QComboBox = spec[1]
                lab = (cmb.currentText() or "").strip()
                val = (cmb.currentData() or "")
                exposed.append((name, str(val)))
                if name.lower() in ("title", "titel", "nadpis"):
                    if lab and lab not in ("- Alle -", "- Vše -", "- Alles -", "- Alle -", "All"):
                        title_label = lab

            elif kind == "radio":
                bg: QButtonGroup = spec[1]
                chosen = ""
                for b in bg.buttons():
                    if b.isChecked():
                        chosen = str(b.property("pf_value") or "")
                        break
                exposed.append((name, chosen))

            elif kind == "checkbox":
                cbs: List[QCheckBox] = spec[1]
                for cb in cbs:
                    if cb.isChecked():
                        exposed.append((name, str(cb.property("pf_value") or "1")))

        return exposed, title_label, year_from, year_to

    def _build_json_path(self, folder: str, title_label: str, year_from: str, year_to: str) -> str:
        def sanitize(x: str) -> str:
            x = (x or "").strip()
            x = re.sub(r"\s+", " ", x)
            x = re.sub(r'[\\/:*?"<>|]', "_", x)
            return x

        title_label = sanitize(title_label) or "Linkliste"
        if year_from and year_to:
            span = f"{sanitize(year_from)}-{sanitize(year_to)}"
        elif year_from:
            span = sanitize(year_from)
        elif year_to:
            span = f"bis_{sanitize(year_to)}"
        else:
            span = "ohne_Zeitraum"

        fname = f"Linkliste {title_label} {span}.json"
        fname = sanitize(fname).replace(" ", "_")
        return os.path.abspath(os.path.join(folder, fname))

    def _start_crawl_current_tab(self):
        folder = (self.ed_folder.text() or "").strip()
        if not folder or not os.path.isdir(folder):
            QMessageBox.warning(self, tr(self.lang, "app_title"), tr(self.lang, "err_folder"))
            return

        tabkey = self._current_tab_key()
        tabdef = next(t for t in TABS if t.key == tabkey)
        spec = self.forms.get(tabkey)
        if not spec:
            QMessageBox.critical(self, tr(self.lang, "app_title"), tr(self.lang, "err_form"))
            return

        self._stop = False
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.lbl_status.setText(tr(self.lang, "running"))

        exposed_items, title_label, y_from, y_to = self._collect_exposed_items(tabkey)

        controls = self.tab_ui[tabkey]["controls"]
        try:
            delay_s = float((controls["__delay__"][1].text() or "0.25").strip())
        except ValueError:
            delay_s = 0.25
        try:
            max_pages = int((controls["__maxpages__"][1].text() or "300").strip())
        except ValueError:
            max_pages = 300

        json_path = self._build_json_path(folder, title_label, y_from, y_to)

        self._append_log(tr(self.lang, "log_prefix", tab=self._tab_title(tabkey)) + f"[Info] JSON: {json_path}")
        self._append_log(tr(self.lang, "log_prefix", tab=self._tab_title(tabkey)) + f"[Info] Exposed: {exposed_items}")

        self._thread = QThread()
        self._worker = CrawlWorker(
            tab=tabdef,
            lang=self.lang,
            action_url=spec["action"],
            exposed_items=exposed_items,
            view_info=spec["view_info"],
            max_pages=max_pages,
            delay_s=delay_s,
            json_path=json_path,
            outdir=folder,
            stop_flag_callable=lambda: self._stop,
        )
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.log.connect(lambda m: self._append_log(tr(self.lang, "log_prefix", tab=self._tab_title(tabkey)) + m))
        self._worker.failed.connect(self._on_failed)
        self._worker.done.connect(self._on_done)

        self._thread.start()

    def _stop_crawl(self):
        self._stop = True
        self.lbl_status.setText(tr(self.lang, "stopping"))
        self._append_log(tr(self.lang, "log_prefix", tab=self._tab_title(self._current_tab_key())) + tr(self.lang, "stopping"))

    def _cleanup_thread(self):
        if self._thread:
            self._thread.quit()
            self._thread.wait(1500)
        self._thread = None
        self._worker = None

    def _on_failed(self, msg: str):
        self._append_log(tr(self.lang, "err", msg=msg))
        QMessageBox.critical(self, tr(self.lang, "app_title"), tr(self.lang, "err", msg=msg))
        self.lbl_status.setText(tr(self.lang, "ready"))
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self._cleanup_thread()

    def _on_done(self, links: list, path: str):
        self._append_log(tr(self.lang, "saved", n=len(links), path=path))
        QMessageBox.information(self, tr(self.lang, "app_title"), tr(self.lang, "saved", n=len(links), path=path))
        self.lbl_status.setText(tr(self.lang, "ready"))
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self._cleanup_thread()


def main():
    app = QApplication([])
    w = MainWindow()
    w.show()
    app.exec()


if __name__ == "__main__":
    main()
