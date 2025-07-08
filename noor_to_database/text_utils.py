from pyarabic.araby import strip_tashkeel, strip_harakat, strip_lastharaka, strip_tatweel
import re

def normalize_arabic(text):
    text = strip_harakat(text)
    text = strip_lastharaka(text)
    text = strip_tatweel(text)
    text = strip_tashkeel(text)
    return text

def remove_xml(text):
    text = re.sub(r'<[^>]+>', '', text)
    text.strip()
    return text

def normalize(text):
    text = remove_xml(text)
    text = normalize_arabic(text)
    return text