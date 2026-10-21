"""Erzeugt die zehnseitige PDF-Präsentation reproduzierbar mit ReportLab.

Aufruf im Projektordner: python scripts/build_presentation.py
Nur für dieses Dokument erforderlich: pip install reportlab
"""

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "docs" / "Praesentation.pdf"
W, H = 960, 540
NAVY = colors.HexColor("#102B46")
TEAL = colors.HexColor("#007F86")
TEXT = colors.HexColor("#17334A")
MUTED = colors.HexColor("#506B80")
PALE = colors.HexColor("#EEF5F7")
FONT = "Helvetica"
BOLD = "Helvetica-Bold"
MONO = "Courier"


def configure_fonts():
    global FONT, BOLD, MONO
    base = Path("C:/Windows/Fonts")
    if (base / "arial.ttf").exists():
        for name, filename in [("Body", "arial.ttf"), ("Strong", "arialbd.ttf"), ("Code", "consola.ttf")]:
            pdfmetrics.registerFont(TTFont(name, str(base / filename)))
        FONT, BOLD, MONO = "Body", "Strong", "Code"


def paragraph(c, text, x, top, width, size=23, color=TEXT, font=None, leading=None):
    style = ParagraphStyle("slide", fontName=font or FONT, fontSize=size,
                           leading=leading or size * 1.27, textColor=color)
    p = Paragraph(text, style)
    _, height = p.wrap(width, H)
    if top - height < 42:
        raise ValueError(f"Text läuft über die Folie hinaus: {text[:60]}")
    p.drawOn(c, x, top - height)
    return top - height


def textline(c, text, x, y, size=20, color=TEXT, font=None):
    c.setFont(font or FONT, size)
    c.setFillColor(color)
    c.drawString(x, y, text)


def base(c, number, title, subtitle=None):
    c.setFillColor(colors.white)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    textline(c, "API-DESIGN UND SYSTEMINTEGRATION", 48, H - 35, 10, MUTED, BOLD)
    paragraph(c, title, 48, H - 68, 870, 34, NAVY, BOLD, 40)
    if subtitle:
        paragraph(c, subtitle, 50, H - 118, 860, 17, MUTED)
    textline(c, "IoT-Echtzeitanalyse mit RabbitMQ", 48, 22, 10, MUTED)
    c.setFont(FONT, 10)
    c.drawRightString(W - 48, 22, f"{number:02d} / 10")


def bullets(c, items, top=365, x=50, width=855, size=23, gap=17):
    y = top
    for index, item in enumerate(items, 1):
        textline(c, f"{index:02d}", x, y - size, 13, TEAL, BOLD)
        y = paragraph(c, item, x + 42, y, width - 42, size)
        y -= gap
    return y


def code(c, lines, x=50, top=350, size=20, color=TEXT):
    for index, line in enumerate(lines):
        textline(c, line, x, top - index * (size * 1.4), size, color, MONO)


def table(c, headers, rows, widths, top=345, rowheight=66, fontsize=21):
    x = 50
    for header, width in zip(headers, widths):
        paragraph(c, header, x, top, width - 15, 15, TEAL, BOLD)
        x += width
    y = top - 42
    for row in rows:
        x = 50
        for cell, width in zip(row, widths):
            paragraph(c, cell, x, y, width - 20, fontsize)
            x += width
        y -= rowheight


def create():
    configure_fonts()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(OUTPUT), pagesize=(W, H), pageCompression=1)
    c.setTitle("IoT-Echtzeitanalyse mit RabbitMQ")
    c.setAuthor("Projektgruppe API-Design und Systemintegration")
    c.setSubject("Bonusaufgabe 1: Architektur, Implementierung und Live-Demo")

    c.setFillColor(NAVY)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    textline(c, "BONUSAUFGABE 1", 58, 460, 15, colors.HexColor("#91DBD7"), BOLD)
    paragraph(c, "IoT-Echtzeitanalyse<br/>mit RabbitMQ", 55, 380, 850, 49, colors.white, BOLD, 58)
    paragraph(c, "Python, Pika und AMQP", 58, 205, 820, 27, colors.HexColor("#BED5E5"))
    textline(c, "Architektur, Fehlerbehandlung und Live-Demonstration", 58, 105, 20, colors.white)
    textline(c, "Gruppenmitglieder: vor dem Vortrag ergänzen", 58, 57, 13, colors.HexColor("#BED5E5"))
    c.showPage()

    base(c, 2, "Architektur und Nachrichtenfluss", "Getrennte Prozesse kommunizieren über RabbitMQ")
    table(c, ["QUELLE", "VERMITTLUNG", "VERARBEITUNG"], [
        ["Drei Sensortypen", "Topic: sensors", "Umgebung / Maschine"],
        ["Analyse-Consumer", "Direct: alarms", "Alarm-Consumer"],
        ["Verarbeitungsereignisse", "Fanout: telemetry", "Monitoring-Consumer"],
        ["Ablehnung / Ablauf", "Direct: dlx", "Dead Letter Queue"],
    ], [305, 245, 315], top=355, rowheight=65, fontsize=21)
    c.showPage()

    base(c, 3, "Routing und Exchange-Wahl", "Der Broker entscheidet, welche Analyse einen Messwert erhält")
    code(c, ["sensors.temperature.temp-01", "Binding Umgebung: sensors.temperature.*", "                  sensors.humidity.*", "Binding Maschine: sensors.vibration.*"], top=350, size=22)
    paragraph(c, "Topic erlaubt fachliche Muster. Direct wählt exakte Alarmstufen. Fanout verteilt alle Monitoringereignisse.", 50, 185, 860, 23)
    paragraph(c, "Alternative: Fanout für Sensoren würde unnötige Kopien in beide Analysequeues senden.", 50, 97, 860, 18, MUTED)
    c.showPage()

    base(c, 4, "JSON-Vertrag und Anomalien", "Pflichtfelder stehen direkt in der Sensornachricht")
    code(c, ["{", '  "sensor_id": "temp-01",', '  "timestamp": "2026-10-21T10:00:00Z",', '  "type": "temperature",', '  "value": 92.5', "}"], x=50, top=345, size=21)
    paragraph(c, "Temperatur<br/>über 80 °C: WARNING<br/>über 100 °C: CRITICAL", 580, 357, 330, 23)
    paragraph(c, "Auch Feuchtigkeit und Vibration besitzen zwei Alarmstufen.", 580, 220, 320, 22)
    paragraph(c, "Grenzwerte sind Annahmen für die Simulation.", 50, 93, 860, 18, MUTED)
    c.showPage()

    base(c, 5, "Senden und Bestätigen", "Brokerannahme und erfolgreiche Verarbeitung sind getrennte Schritte")
    code(c, ["channel.confirm_delivery()", "properties = pika.BasicProperties(", "    delivery_mode=2)", "channel.basic_publish(", "    ..., properties=properties, mandatory=True)"], top=350, size=21)
    paragraph(c, "Durable Topologie und persistente Nachrichten sichern den Brokerpfad. Der Consumer bestätigt seine Verarbeitung anschließend manuell.", 50, 182, 860, 23)
    paragraph(c, "Nach dem ersten Versuch höchstens fünf Retries: 1, 2, 4, 8 und 10 Sekunden.", 50, 89, 860, 18, MUTED)
    c.showPage()

    base(c, 6, "Aggregation und Fehlerpfad", "Temperaturwerte bleiben bis zum Fensterabschluss unbestätigt")
    bullets(c, [
        "Zehn Sekunden Empfangszeit bilden ein Fenster. Der Timer läuft auch ohne neue Nachricht.",
        "Das bestätigte Aggregat enthält Anzahl, Mittelwert, Minimum und Maximum.",
    ], top=365, width=420, size=21, gap=23)
    code(c, ["_telemetry(channel, topology, event)", "for pending in window.pending:", "    _telemetry(", "        channel, topology,", "        pending.event)", "    channel.basic_ack(", "        delivery_tag=pending.tag)"], x=505, top=345, size=17)
    code(c, ["channel.basic_nack(delivery_tag=tag, requeue=False)"], x=50, top=138, size=20)
    paragraph(c, "Dauerhafte Fehler nutzen nach begrenzter Fehlerbehandlung den DLX. Code aus flush_window und _reject, gekürzt.", 50, 100, 860, 18, MUTED)
    c.showPage()

    base(c, 7, "Priorität, TTL und Monitoring")
    table(c, ["MECHANISMUS", "KONFIGURATION", "WIRKUNG"], [
        ["Alarmpriorität", "Queue: max 9<br/>Nachricht: 3 / 9", "CRITICAL vor wartenden WARNING"],
        ["Frische der Daten", "TTL: 60.000 ms", "Abgelaufene Werte in die DLQ"],
        ["Monitoring", "Eigene Telemetriequeue", "Erfolge, Typen, Alarme,<br/>Fehler und mittlere Zeit"],
    ], [255, 265, 345], top=355, rowheight=90, fontsize=21)
    paragraph(c, "Prefetch 1 begrenzt die Vorabauslieferung der Alarme. Laufende Verarbeitung wird nicht unterbrochen.", 50, 84, 860, 18, MUTED)
    c.showPage()

    base(c, 8, "Live-Demonstration", "Vier Minuten im vorbereiteten Terminal")
    code(c, ["python -m iot_analysis run", "python -m iot_analysis demo --scenario invalid", "python -m iot_analysis demo --scenario crash", "python -m iot_analysis demo --scenario priority"], top=350, size=21)
    bullets(c, ["Normalbetrieb und Zehn-Sekunden-Aggregat beobachten.", "DLQ-Eintrag und erneute Zustellung nach Consumer-Ausfall zeigen."], top=200, size=21, gap=14)
    c.showPage()

    base(c, 9, "Verhalten in den Fehlerszenarien")
    table(c, ["AUSLÖSER", "BESTÄTIGUNG", "NACHRICHTENWEG"], [
        ["Ungültige Nachricht", "NACK, requeue=False", "DLX und DLQ"],
        ["Consumer endet vor ACK", "Weder ACK noch NACK", "Erneute Zustellung"],
        ["Dauerhafter Prozessfehler", "Nach 3 Versuchen NACK", "DLX und DLQ"],
    ], [300, 285, 280], top=355, rowheight=85, fontsize=21)
    paragraph(c, "Ein Consumer-Absturz allein führt bei Classic-Queues zunächst zur Requeue unbestätigter Lieferungen.", 50, 95, 860, 20, MUTED)
    c.showPage()

    base(c, 10, "Erkenntnisse und Grenzen")
    bullets(c, [
        "Broker-Routing trennt die Fachanalysen. Eigene Alarme und Telemetrie halten Dienste unabhängig.",
        "ACKs, begrenzte Versuche und DLQ machen das Verhalten bei bekannten Fehlern nachvollziehbar.",
        "Mehrfachzustellung bleibt möglich. Ein einzelner Broker und Auswertungen im RAM begrenzen die Lösung.",
    ], top=365, size=23, gap=22)
    paragraph(c, "Nächste Schritte: persistente idempotente Verarbeitung und Brokerreplikation nach Bedarf.", 50, 94, 860, 20, TEAL)
    c.showPage()
    c.save()
    print(OUTPUT)


if __name__ == "__main__":
    create()
