import pandas as pd

#leitura do arquivo CSV, com definição de separador tab
arquivo = pd.read_csv("datasheet_oscars.csv", sep="\t")

for linha in arquivo.itertuples(index=False):
    print(linha.Year)
