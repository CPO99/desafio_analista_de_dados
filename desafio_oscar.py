import pandas as pd
import psycopg2 as pg

#conexão com o banco de dados
try:
    conexao = pg.connect(
        database="oscar",
        user="postgres",
        password="ifrn",
        host="localhost",  
        port="5432"        
    )
    
    cursor = conexao.cursor()

#se ocorrer qualquer erro na conexão com o banco, o progama será encerrado    
except Exception as e:
    print("Erro ao conectar ao banco:", e)
    
    exit()

#leitura do arquivo CSV, com definição de separador tab, e garantia da correta leitura dos dados no padrão utf-8
arquivo = pd.read_csv("datasheet_oscars.csv", sep="\t", encoding='utf-8')
#padronização do titulo das colunas para minúsculo, retirando espaços antes e depois do nome, se houver
arquivo.columns = arquivo.columns.str.upper().str.strip()

def validar_existencia(valor):
    if not (pd.isna(valor) or valor == "nan" or valor == "none" or valor == None or valor == ""):
        return True
    else:
        False

def validador_tbl_movie(title: str):
    if validar_existencia(title):
        #tratamentos para o título ficar padronizado no banco
        title = " ".join(str(title).split()).lower()
        
        return title
    else:
        return "NONE" 
        

def validador_tlb_oscar(year: int, ceremony: int):
    if validar_existencia(ceremony):
        ceremony = int(ceremony)
    else:
        ceremony = "NONE"

    if validar_existencia(year):
        year = str(year)

        if "/" in year:
            year = int(year[:year.find("/")])
        else:
            year = int(year)
            
        return year, ceremony
    else:
        year = "NONE"

def validador_tbl_class(class_description: str):
    if validar_existencia(class_description):
        return " ".join(str(class_description).split()).lower()
    else:
        return "NONE"

def validador_tbl_category(category):
    if validar_existencia(category):
        return " ".join(str(category).split()).lower()
    else:
        return "NONE"

def validador_tbl_nominees(name, nominees, winner, detail, note):
    if validar_existencia(name):
        name = " ".join(str(name).split()).lower()
    else:
        name = "NONE"
    
    if validar_existencia(nominees):
        nominees = " ".join(str(nominees).split()).lower()
    else:
        nominees = "NONE"

    if validar_existencia(winner):
        winner = True
    else:
        winner = False

    if validar_existencia(detail):
        detail = " ".join(str(detail).split()).lower()
    else:
        detail = "NONE"

    if validar_existencia(note):
        note = " ".join(str(note).split()).lower()
    else:
        note = "NONE"

    return name, nominees, winner, detail, note

    return note

def importador(movie, year, ceremony, class_description, category, name, nominees, winner, detail, note):
    


#iterando sobre as linhas da tabela a ser importada
for linha in arquivo.itertuples(index=False):
    ceremony = linha.CEREMONY #okay
    year = linha.YEAR #okay
    class_description = linha.CLASS
    category = linha.CATEGORY
    movie = linha.MOVIE #okay
    name = linha.NAME
    nominees = linha.NOMINEES
    winner = linha.WINNER
    detail = linha.DETAIL
    note = linha.NOTE

    #validador_tbl_movie - retorno de erro ou nome do filme tratado
    movie = validador_tbl_movie(movie)

    #validador_tlb_oscar - retorno de erro ou ano e cerimônia tratados
    year, ceremony = validador_tlb_oscar(year, ceremony)

    #validador_tbl_class - retorno de erro ou class tratada
    class_description = validador_tbl_class(class_description)

    #validador_tbl_class - retorno de erro ou categoria tratada
    category = validador_tbl_category(category)

    name, nominees, winner, detail, note = validador_tbl_nominees(name, nominees, winner, detail, note)

    importador(movie, year, ceremony, class_description, category, name, nominees, winner, detail, note)
        
