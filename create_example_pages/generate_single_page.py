import sys
import os
import random
import datetime
from faker import Faker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import EXAMPLE_RESULTS_PAGES

fake = Faker("ru_RU")

MAX_NAME_LEN = 10

# Список категорий, каждая с заголовком <h2> и типом footer
categories = [
    {"title": "Ж10, 12 КП, 1,6 км", "footer_has_qual": True},
    {"title": "Ж12, 12 КП, 1,8 км", "footer_has_qual": True},
    {"title": "Ж14, 16 КП, 2,3 км", "footer_has_qual": True},
    {"title": "Ж16, 20 КП, 2,8 км", "footer_has_qual": False},
    {"title": "Ж20, 24 КП, 3 км", "footer_has_qual": False},
    {"title": "Ж35, 20 КП, 2,8 км", "footer_has_qual": False},
    {"title": "Ж55, 16 КП, 2,3 км", "footer_has_qual": False},
    {"title": "ЖЭ, 24 КП, 3 км", "footer_has_qual": False},
    {"title": "М10, 12 КП, 1,8 км", "footer_has_qual": True},
    {"title": "М12, 16 КП, 2,3 км", "footer_has_qual": True},
    {"title": "М14, 20 КП, 2,8 км", "footer_has_qual": True},
    {"title": "М16, 24 КП, 3 км", "footer_has_qual": False},
    {"title": "М20, 30 КП, 3,5 км", "footer_has_qual": False},
    {"title": "М35, 30 КП, 3,5 км", "footer_has_qual": False},
    {"title": "М55, 20 КП, 2,8 км", "footer_has_qual": False},
    {"title": "МЭ, 30 КП, 3,5 км", "footer_has_qual": False},
    {"title": "Новички, 22 КП, 2 км", "footer_has_qual": False},
]

TEAMS = [
    "СШ 18 АТЛЕТ", "СШОР 18 Дон спорт", "СШ 18 Юго-Запад", "СШ 18 ОРИОН",
    "СШОР 18 Олимп", "СШ 18 Берёзовая рощ", "СШ 18 Вильденберг",
    "Углянец-РФ", "Воронеж", "СШ №18 Азимут", "СШ 18 ИМПУЛЬС", "СШОР 18 Смородино",
    "СШ 18 Паровоз", "СШОР 18 Макейчик", "СШ 18 Богданка", "Грачевы", "СИНТЕЗ", "СШ 18 ОК",
    "ВУНЦ ВВС ВВА"
]

QUAL_CLASSES = ["Iю", "IIю", "IIIю", "I", "II", "III", "КМС", "МС"]

def format_time(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

def generate_participants(total_count: int, year_range: tuple, missing_count: int):
    full_count = total_count - missing_count
    participants = []

    for _ in range(full_count):
        first = fake.first_name()[:MAX_NAME_LEN]
        last = fake.last_name()[:MAX_NAME_LEN]
        team = random.choice(TEAMS)
        qualification = random.choice(QUAL_CLASSES)
        bib = random.randint(200, 500)
        year = random.randint(year_range[0], year_range[1])
        total_sec = random.randint(900, 3600)
        participants.append({
            "first_name": first,
            "last_name": last,
            "team": team,
            "qualification": qualification,
            "bib": bib,
            "year": year,
            "total_seconds": total_sec,
            "result_str": "",
            "offset_str": "",
            "rank": "",
        })

    participants.sort(key=lambda x: x["total_seconds"] or 999999)
    leader_time = participants[0]["total_seconds"] if full_count > 0 else 0

    for idx, p in enumerate(participants[:full_count], start=1):
        p["rank"] = idx
        p["result_str"] = format_time(p["total_seconds"])
        offset = p["total_seconds"] - leader_time
        hours = offset // 3600
        mins = (offset % 3600) // 60
        secs = offset % 60
        if hours > 0:
            p["offset_str"] = f"+{hours}:{mins:02d}:{secs:02d}"
        else:
            p["offset_str"] = f"+{mins:02d}:{secs:02d}"

    for _ in range(missing_count):
        first = fake.first_name()[:MAX_NAME_LEN]
        last = fake.last_name()[:MAX_NAME_LEN]
        team = random.choice(TEAMS)
        qualification = random.choice(QUAL_CLASSES)
        bib = random.randint(200, 500)
        year = random.randint(year_range[0], year_range[1])
        participants.append({
            "first_name": first,
            "last_name": last,
            "team": team,
            "qualification": qualification,
            "bib": bib,
            "year": year,
            "total_seconds": None,
            "result_str": "",
            "offset_str": "",
            "rank": ""
        })

    for idx, p in enumerate(participants, start=1):
        p["place"] = idx

    return participants

def generate_footer_qual():
    qual_level = random.randint(20, 600)
    perc_Iu = random.randint(110, 200)
    perc_IIu = perc_Iu + random.randint(10, 40)
    base = random.randint(1200, 3000)
    time_Iu = format_time(int(base * perc_Iu / 100))
    time_IIu = format_time(int(base * perc_IIu / 100))
    return (
        f"Квалификационный уровень - {qual_level} баллов",
        f"Iю     - {perc_Iu}%  -  {time_Iu}",
        f"IIю    - {perc_IIu}%  -  {time_IIu}",
    )

def generate_page_for_date(event_date: datetime.date, output_dir="results_pages"):
    html_parts = [f"""<html><head>
<meta content="text/html; charset=windows-1251" http-equiv="Content-Type">
<title>WinOrient - Result list</title>
<style type="text/css">
body {{ margin-left:10;margin-top:10; }}
A:hover {{color:#FF0000;}}
H1 {{font-family: Arial, Helvetica, sans-serif;font-size: 12pt;font-weight: bold;color: #333366;text-align: center;}}
H2 {{font-family: Arial, Helvetica, sans-serif;font-size: 12pt;font-weight: bold;color: #FF0000;text-align: left;}}
p, .text {{font-family: Arial, Helvetica, sans-serif;font-size: 9pt;color: #000000;text-align: justify;}}
</style>
</head><body link="#00A5DE" vlink="#00A5DE">
<h1>Чемпионат и первенство МБУДО СШ №18<br>
по спортивному ориентированию<br>
кросс-спринт-общий старт<br>
{event_date.strftime('%d.%m.%Y')}, г. Воронеж<br><br>ПРОТОКОЛ РЕЗУЛЬТАТОВ</h1>"""]

    for cat in categories:
        html_parts.append(f"<h2>{cat['title']}</h2>")
        html_parts.append("<pre>")
        has_vyp = any(prefix in cat["title"] for prefix in ["Ж10", "Ж12", "Ж14", "М10", "М12", "М14"])
        header = ("<u><b>№п/п Фамилия, имя              Коллектив            Квал Номер ГР   "
                  "РезультатОтставан    Место Вып  Прим </b></u>"
                  if has_vyp else
                  "<u><b>№п/п Фамилия, имя              Коллектив            Квал Номер ГР   "
                  "РезультатОтставан    Место Прим </b></u>")
        html_parts.append(header)

        total = random.randint(10, 30)
        missing = random.randint(0, 5)
        participants = generate_participants(total, (1900, 2017), missing)

        for p in participants:
            name = f"{p['last_name']} {p['first_name']}"
            if p["total_seconds"] is not None:
                row = (
                    f"{p['place']:>3} {name:<20} {p['team']:<20} {p['qualification']:<4} "
                    f"{p['bib']:>4} {p['year']:>4} {p['result_str']}   {p['offset_str']:>6}   "
                    f"{p['place']:>2}  {p['qualification'] if has_vyp else ''}"
                )
            else:
                row = (
                    f"{p['place']:>3} {name:<20} {p['team']:<20} {p['qualification']:<4} "
                    f"{p['bib']:>4} {p['year']:>4} {'':<8} {'':>6}   {'':>2}  "
                    f"{p['qualification'] if has_vyp else ''}"
                )
            html_parts.append(row)

        if cat["footer_has_qual"]:
            html_parts.extend(generate_footer_qual())
        else:
            html_parts.append("Ранг не определялся")

        html_parts.append("</pre>")

    html_parts.append("<pre>Главный судья                                   ")
    html_parts.append("Главный секретарь                              </pre>")
    html_parts.append("</body></html>")

    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"{event_date.strftime('%Y%m%d')}_rez.html")
    with open(filename, "w", encoding="cp1251") as f:
        f.write("\n".join(html_parts))
    print(f"Создан файл: {filename}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_single_page.py YYYY-MM-DD")
        sys.exit(1)

    try:
        input_date = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except ValueError:
        print("Неверный формат даты. Используйте YYYY-MM-DD.")
        sys.exit(1)

    generate_page_for_date(input_date, output_dir=EXAMPLE_RESULTS_PAGES)
