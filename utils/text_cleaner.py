import re


def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = re.sub(r"[\n\r\t]+", " ", text)
    text = re.sub(r"[^\w\s\-.,]", "", text)
    text = re.sub(r" +", " ", text).strip()
    return text

def clean_city_name(city: str) -> str:
    if not city:
        return None
    
    city = re.sub(r"(^[\s\-]+)|([\s\-]+$)", "", city)
    city = re.sub(r"\s+", " ", city)
    city = re.sub(r"[^А-ЯЁа-яё\-\s]", "", city)
    
    if city:
        city = city.strip()
        parts = [p.capitalize() for p in city.split()]
        city = " ".join(parts)
        city = re.sub(r"\bИ\b", "и", city)

    city = city if city and len(city) >= 3 else None
    return city