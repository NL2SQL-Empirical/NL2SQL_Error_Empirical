import spacy


nlp = spacy.load("en_core_web_sm")

def is_date(date_str) -> bool:
    doc = nlp(date_str)
    for ent in doc.ents:
        if ent.label_ == 'DATE':
            return True
    return False
    
def is_time(time_str) -> bool:
    doc = nlp(time_str)
    for ent in doc.ents:
        if ent.label_ == 'TIME':
            return True
    return False

def is_number(number_str) -> bool:
    doc = nlp(number_str)
    for ent in doc.ents:
        if ent.label_ == 'CARDINAL':
            return True
    return False