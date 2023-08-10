from googletrans import Translator

def translate_text(text):
    service_urls = [
        'translate.google.com',
        'translate.google.co.vn',
    ]

    translator = Translator(service_urls=service_urls)
    translation = translator.translate(text, src='en', dest='vi')
    translated_text = translation.text
    return translated_text
    
print(translate_text("hello"))
