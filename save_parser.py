from queryParser import FastQueryParser
import pickle

parser = FastQueryParser()
with open("models/query_parser.pkl", "wb") as f:
    pickle.dump(parser, f)