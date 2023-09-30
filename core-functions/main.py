import torch
from transformers import BertTokenizer, BertModel

import logging
logging.basicConfig(level=logging.INFO)

import matplotlib.pyplot as plt

tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')

text = "A deliciously crunchy sandwich made from bread, ham, and cheese."
tagged_text = "[CLS] " + text + " [SEP]"
tokenized_text = tokenizer.tokenize(tagged_text)
print(tokenized_text)