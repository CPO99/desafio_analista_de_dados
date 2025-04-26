import pandas as pd

#leitura do arquivo CSV, com definição de separador tab, e garantia da correta leitura dos dados no padrão utf-8
arquivo = pd.read_csv("datasheet_oscars.csv", sep="\t", encoding='utf-8')
#padronização do titulo das colunas para minúsculo, retirando espaços antes e depois do nome, se houver
arquivo.columns = arquivo.columns.str.lower().str.strip()


def tbl_movie(title: str):
    if pd.isna(title) or title == "nan" or title == "none" or title == None or title == "":
        return False, "[ERRO] - NOME DO FILME NÃO INFORMADO"
    else:
        #tratamentos para o título ficar padronizado no banco
        title = " ".join(title.split()).lower()
        
        return True, title
        

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

    ver, movie = tbl_movie(movie)

    print(movie)
        
