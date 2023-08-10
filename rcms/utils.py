from pyvi import ViTokenizer

# Function to tokenize and extract desired POS tags from Vietnamese text using PyVi
def tokenize_and_extract_words(text):
    try:
        words = ViTokenizer.tokenize(text).split(" ")
        return words
    except IndexError as e:
        print("IndexError:", e)
        return []

# Function to normalize a list of vectors
def normalize_vector(vector):
    norm = sum([val**2 for val in vector]) ** 0.5
    return [val / norm for val in vector]
