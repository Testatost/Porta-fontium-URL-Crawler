# PortaFontium – Link-Crawler (PySide6)
Kleines PySide6-GUI-Tool, das auf **PortaFontium** pro Kategorie (Matriken, Chroniken, Urkunden, Fotos, Volkszählung, Karten, Zeitungen/Periodika, Amtsbücher) Suchergebnisse seitenweise crawlt, Links extrahiert und als **JSON** speichert.

## Features
- Mehrere Tabs je Datentyp (Matriken, Chroniken, Urkunden, Fotos, Volkszählung, Karten, Zeitungen/Periodika, Amtsbücher)
- Formulare/Felder werden dynamisch von PortaFontium geladen (Drupal Views Exposed Form)
- Pagination via GET, Fallback via Drupal `/views/ajax`
- DE/CZ Umschaltung (UI + einfache DE-Normalisierung von Labels/Optionen)
- Exportdatei: `Linkliste_<Titel>_<Zeitraum>.json`
- Periodika: expandiert `/periodical/`-Seiten zu einzelnen `/iipimage/<id>`-Ausgaben

## Voraussetzungen
- Python 3.10+ empfohlen
- Zugriff auf `https://www.portafontium.eu`
- Hinweis: `PF_VERIFY = False` ist gesetzt (TLS-Verify aus). Wenn TLS bei dir ok ist: auf `True` stellen.

## Installation
~~~bash
git clone <REPO-URL>
cd <REPO-ORDNER>

python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows PowerShell

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
~~~

## requirements.txt
Lege im Repo eine Datei `requirements.txt` an mit:
~~~txt
PySide6>=6.5
requests>=2.31
beautifulsoup4>=4.12
lxml>=5.0
urllib3>=2.0
~~~

## Start
~~~bash
python main.py
~~~

## Nutzung
1. Speicherordner auswählen
2. Tab auswählen
3. Filter setzen (optional)
4. „Crawl starten“
5. JSON wird im gewählten Ordner gespeichert

## Output
Beispiel-JSON:
~~~json
[
  {
    "url": "https://www.portafontium.eu/iipimage/123456",
    "outdir": "/pf/output",
    "pages": ""
  }
]
~~~

## Hinweise
- „Delay (s)“ und „Max Seiten“ steuern Geschwindigkeit und Laufzeit.
- Änderungen an PortaFontium (HTML/Views) können Anpassungen am Parsing erfordern.
