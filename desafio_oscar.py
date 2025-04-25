import pandas as pd

#leitura do arquivo CSV, com definição de separador tab
arquivo = pd.read_csv("datasheet_oscars.csv", sep="\t")
#padronização do nome das colunas para minúsculo, retirando espaços antes e depois do nome, se houver
arquivo.columns = arquivo.columns.str.lower().str.strip()


def tbl_movie():
    pass

def tlb_oscar():
    pass

def tbl_class():
    pass

def tbl_category():
    pass

def tbl_nominees():
    pass

#iterando sobre as linhas da tabela a ser importada
for linha in arquivo.itertuples(index=False):
    year = linha.year
    category = linha.category
    movie = linha.movie
    name = linha.name
    nominees = linha.nominees
    winner = linha.winner
    detail = linha.detail
    note = linha.note

    print(year, movie)
