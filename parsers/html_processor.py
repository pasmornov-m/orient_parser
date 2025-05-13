from bs4 import BeautifulSoup
from utils.text_cleaner import clean_text, clean_city_name
from datetime import date
import re
from pyspark.sql import functions as F
from schemas.spark_schemas import EVENT_SCHEMA, DIST_SCHEMA, RESULTS_SCHEMA


class VRNFSO_html_processor_spark:

    _results_re = re.compile(r'''
            ^\s*(\d+)\s+                # №п/п
            ([А-ЯЁ][а-яё-]+\s[А-ЯЁ][а-яё-]+)\s+  # Фамилия и имя
            (.*?)\s{2,}                 # Коллектив
            ([А-Яa-zIЮМСК]+)?\s*        # Квал (может отсутствовать)
            (\d+)\s+                    # Номер
            (\d{4})\s+                  # Год рождения
            (\d{2}:\d{2}:\d{2})\s+      # Результат
            (\+\d{2}:\d{2})\s+          # Отставание
            (=?\s*\d+)\s*               # Место
            (.*)                        # Примечание (если есть)
        ''', re.VERBOSE | re.IGNORECASE)
    
    def __init__(self, url_date, html_page, spark):
        self.url_date = url_date
        self.date_str = self.get_date_str()
        self.date = self.get_date()
        self.soup = BeautifulSoup(html_page, "html.parser")
        self.spark = spark
        self.relay_dates_list = []

        if self.soup:
            self.blocks = list(self.soup.find_all("h2"))
    
    def get_date_str(self):
        year = self.url_date[:4]
        month = self.url_date[4:6]
        day = self.url_date[6:8]
        return f"{day}.{month}.{year}"
    
    def get_date(self):
        year = int(self.url_date[:4])
        month = int(self.url_date[4:6])
        day = int(self.url_date[6:8])
        return date(year, month, day)
    
    def _parse_result_line(self, line, group):
        m = self._results_re.match(line)
        if not m:
            return None
        field = m.groups()

        postion_number = int(field[0])
        full_name = field[1]
        team = field[2]
        qualification = field[3]
        bib_number = int(field[4])
        birth_year = int(field[5])
        result_time = field[6]
        time_gap = field[7]
        finish_position = int(field[8].replace('=', '').strip())

        return (
            self.date,
            group,
            postion_number,
            full_name,
            team,
            qualification,
            bib_number,
            birth_year,
            result_time,
            time_gap,
            finish_position,
        )

    def _extract_event_name(self, title):
        raw = title.split(self.date_str)[0] if self.date_str in title else title
        return clean_text(raw)

    def _extract_city(self, text):
        pattern = r"(?i)г\.?\s*([А-ЯЁ][а-яё\-\s]+)(?=\s|$|,|\.|<|\))"
        candidates = [
            clean_city_name(m.group(1)).split()[0]
            for m in re.finditer(pattern, text)
            if clean_city_name(m.group(1)) and len(clean_city_name(m.group(1))) >= 3
            and not any(ch.isdigit() for ch in m.group(1))
        ]
        return candidates[-1] if candidates else None

    def _parse_distance_info(self, h2_text):
        match = re.match(r"(.+),\s*(\d+)\s*КП,\s*([\d.,]+)\s*(км|м)", h2_text)
        if match:
            group, kp, length, unit = match.groups()
            length = float(length.replace(",", "."))
            if unit == "м":
                length /= 1000
            return group, int(kp), round(length, 2)
        return None
    
    def _iterate_category_blocks(self):
        for h2 in self.blocks:
            group = h2.get_text(strip=True).split(",")[0]
            pre = h2.find_next("pre")
            if pre:
                yield group, pre.get_text().splitlines()
    
    def _is_valid_html(self, required_tag: str) -> bool:
        return self.soup is not None and self.soup.find(required_tag) is not None

    
    def parse_events(self):
        if not self._is_valid_html("h1") or not self.date:
            return None
        
        title_text = " ".join(self.soup.find("h1").stripped_strings)
        event_name = self._extract_event_name(title_text)
        city = self._extract_city(title_text)

        if "эстафета" in event_name.lower():
            self.relay_dates_list.append(self.date)
            return None
        
        data = [(event_name, self.date, city)]
        df_events = self.spark.createDataFrame(data, EVENT_SCHEMA)
        df_events = (df_events.select(
            F.substring(F.col("event_name"), 1, 100).alias("event_name"),
            "event_date",
            F.substring(F.col("city"), 1, 50).alias("city")
            ).distinct())
        return df_events
        
    
    def parse_distances(self):
        if not self._is_valid_html("h2") or self.date in self.relay_dates_list:
            return None
        if self.date in self.relay_dates_list:
            return
        
        distances = []
        
        for h2 in self.blocks:
            text = h2.get_text()
            parsed = self._parse_distance_info(text)
            if parsed:
                group, kp, length = parsed
                distances.append((self.date, group, kp, length))
        
        df_distances = self.spark.createDataFrame(distances, DIST_SCHEMA)
        df_distances = (df_distances.select(
            "event_date",
            F.substring(F.col("group_name"), 1, 20).alias("group_name"),
            F.col("cp"),
            "length_km"
            ).distinct())
        return df_distances

    
    def parse_results(self):
        if not self._is_valid_html("h2") or self.date in self.relay_dates_list:
            return None
        
        results_data = []

        for group, lines in self._iterate_category_blocks():
            for line in lines:
                parsed = self._parse_result_line(line, group)
                if parsed:
                    results_data.append(parsed)
        
        df_results = self.spark.createDataFrame(results_data, RESULTS_SCHEMA)
        df_results = (df_results.select(
            "event_date",
            "group_name",
            "position_number",
            "full_name",
            "team",
            F.substring(F.col("qualification"), 1, 10).alias("qualification"),
            "bib_number",
            "birth_year",
            "finish_position",
            "result_time",
            "time_gap"))
        return df_results
    
    def parse_all(self):
        if not self.soup:
            return None, None, None
        try:
            df_events = self.parse_events()
            df_distances = self.parse_distances()
            df_results = self.parse_results()
            return df_events, df_distances, df_results
        except Exception as e:
            print("parse_all error: ", e)
            return None, None, None