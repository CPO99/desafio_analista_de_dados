import pandas as pd
import psycopg2 as pg
from datetime import datetime

horario_inicio = datetime.now()

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


#limpeza da base de dados para teste

remover_tbl_nominees = "DELETE FROM public.tbl_nominees"
cursor.execute(remover_tbl_nominees)

remover_tbl_movie = "DELETE FROM public.tbl_movie"
cursor.execute(remover_tbl_movie)

remover_tbl_oscar = "DELETE FROM public.tbl_oscar"
cursor.execute(remover_tbl_oscar)

remover_tbl_class = "DELETE FROM public.tbl_class"
cursor.execute(remover_tbl_class)

remover_tbl_category = "DELETE FROM public.tbl_category"
cursor.execute(remover_tbl_category)

#guarda os dados não registrados e o motivo
dados_nao_import = []

#guarda a quantidade de registros importados com sucesso
qtd_dados_import = 0

ceremony_year = {}

#leitura do arquivo CSV, com definição de separador tab, e garantia da correta leitura dos dados no padrão utf-8
arquivo = pd.read_csv("datasheet_oscars.csv", sep="\t", encoding='utf-8')
#padronização do titulo das colunas para minúsculo, retirando espaços antes e depois do nome, se houver
arquivo.columns = arquivo.columns.str.upper().str.strip()

def validar_existencia(valor):
    if not (pd.isna(valor) or valor == "nan" or valor == "none" or valor == None or valor == ""):
        return True
    else:
        False

def validador_tbl_movie(title: str, category: str):
    if validar_existencia(title):
        #tratamentos para o título ficar padronizado no banco
        title = " ".join(str(title).split()).lower()
        
        return title
    else:
        categorias_sem_filme = [
            'honorary award',
            'irving g. thalberg memorial award',
            'scientific and technical award (scientific and engineering award)',
            'scientific and technical award (technical achievement award)',
            'john a. bonner medal of commendation',
            'jean hersholt humanitarian award',
            'gordon e. sawyer award',
            'scientific and technical award (academy award of merit)',
            'award of commendation',
            'scientific and technical award (special award)',
            'special award'
            ]

        if category not in categorias_sem_filme:
            return False
        else:
            return "N/A"
            
        
#essa função normaliza ano e cerimônia,
#bem como se não informado ano ou cerimônia
#o código tenta encontrar o ano e a cerimônia
#com base nos registros anteriores
#pois são salvos conjuntos únicos de ano e cerimônia
#em um dicionário
def validador_tlb_oscar(year: int, ceremony: int):
    if validar_existencia(ceremony):
        ceremony = int(ceremony)
        if ceremony not in list(ceremony_year.keys()):
            if validar_existencia(year):
                year = str(year)
                if "/" in year:
                    year = int(year[:year.find("/")])
                    ceremony_year[ceremony] = year

                    return year, ceremony
                    
                else:
                    year = int(year)
                    ceremony_year[ceremony] = year

                    return year, ceremony 
            else:
                return False, ceremony
        else:
            return ceremony_year[ceremony], ceremony
    else:
        if validar_existencia(year):
            year = str(year)
            if "/" in year:
                year = int(year[:year.find("/")])

                chave = [chave for chave, item in ceremony_year if item == year]

                if len(chave) == 1:
                    return year, chave[0]
                else:
                    return year, False
            else:
                year = int(year)
                    
                chave = [chave for chave, item in ceremony_year if item == year]

                if len(chave) == 1:
                    return year, chave[0]
                else:
                    return year, False
        else:
            return False, False
                                          
def validador_tbl_class(class_description: str):
    if validar_existencia(class_description):
        return " ".join(str(class_description).split()).lower()
    else:
        return False

def validador_tbl_category(category):
    if validar_existencia(category):
        return " ".join(str(category).split()).lower()
    else:
        return None
    
def validador_name(name, category, nominees):
    if validar_existencia(name):
        name = " ".join(str(name).split()).lower()

        return name

    else:
        categorias_sem_pessoa = [
            'animated feature film',
            'art direction',
            'cinematography',
            'costume design',
            'documentary (feature)',
            'documentary (short subject)',
            'film editing',
            'makeup',
            'music (original score)',
            'music (original song)',
            'best picture',
            'short film (animated)',
            'short film (live action)',
            'sound editing',
            'sound mixing',
            'visual effects',
            'makeup and hairstyling',
            'production design',
            'sound',
            'documentary feature film',
            'documentary short film',
            'animated short film',
            'scientific and technical award (academy award of merit)',
            'scientific and technical award (scientific and engineering award)',
            'scientific and technical award (special award)',
            'scientific and technical award (technical achievement award)',
            'honorary award'
            ]
    
        if category not in categorias_sem_pessoa:
            if validar_existencia(nominees):
                name = " ".join(str(nominees).split()).lower()
                return name
            else:
                return False
        else:
            return "N/A"

def validar_nominees(nominees, category, name):
    if validar_existencia(nominees):
        nominees = " ".join(str(nominees).split()).lower()

        return nominees
    else:
        categorias_sem_nomeado = [
            'foreign language film',
            'international feature film',
            ]
        if category not in categorias_sem_nomeado:
            if validar_existencia(name):
                nominees = " ".join(str(name).split()).lower()
                return nominees
            else:
                return False
        else:
            return "N/A"
    
def validador_tbl_nominees(winner, detail, note):
    #validando winner
    if validar_existencia(winner):
        if type(winner) == type(True):
            if winner == True:
                winner = True
            else:
                winner = False
        else:
            if "true" == winner.lower():
                winner = True
            elif "false" == winner.lower():
                winner = False
            else:
                winner = "erro"        
    else:
        winner = False

    #validando detail
    if validar_existencia(detail):
        detail = " ".join(str(detail).split()).lower()
    else:
        detail = None

    #validando note
    if validar_existencia(note):
        note = " ".join(str(note).split()).lower()
    else:
        note = None

    return winner, detail, note

def importador(movie, year, ceremony, class_description, category, name, nominees, winner, detail, note):
    id_registro_tbl_movie = 0
    id_registro_tbl_oscar = 0
    id_registro_tbl_class = 0
    id_registro_tbl_category = 0

    #tbl_movie
    verificar_registro = "SELECT COUNT(*) FROM tbl_movie WHERE title = %s"
    cursor.execute(verificar_registro, (movie,))

    retorno_consulta = cursor.fetchone()

    if retorno_consulta[0] == 0:
        registrar_movie = "INSERT INTO tbl_movie (title) VALUES (%s) RETURNING ID"
        
        cursor.execute(registrar_movie, (movie,))
        
        id_registro_tbl_movie = cursor.fetchone()[0]
    else:
        obter_id = "SELECT id FROM tbl_movie WHERE title = %s"
        cursor.execute(obter_id, (movie,))

        id_registro_tbl_movie = cursor.fetchone()[0]

    #tbl_oscar
    verificar_registro = "SELECT COUNT(*) FROM tbl_oscar WHERE year = %s AND ceremony = %s"
    cursor.execute(verificar_registro, (year,ceremony,))

    retorno_consulta = cursor.fetchone()

    if retorno_consulta[0] == 0:
        registrar_oscar = "INSERT INTO tbl_oscar (year, ceremony) VALUES (%s, %s) RETURNING ID"
        cursor.execute(registrar_oscar, (year,ceremony,))
        id_registro_tbl_oscar = cursor.fetchone()[0]
    else:
        obter_id = "SELECT id FROM tbl_oscar WHERE year = %s AND ceremony = %s"
        cursor.execute(obter_id, (year,ceremony,))

        id_registro_tbl_oscar = cursor.fetchone()[0]

    #tbl_class
    verificar_registro = "SELECT COUNT(*) FROM tbl_class WHERE description = %s"
    cursor.execute(verificar_registro, (class_description,))

    retorno_consulta = cursor.fetchone()

    if retorno_consulta[0] == 0:
        registrar_class = "INSERT INTO tbl_class (description) VALUES (%s) RETURNING ID"
        cursor.execute(registrar_class, (class_description,))
        id_registro_tbl_class = cursor.fetchone()[0]
    else:
        obter_id = "SELECT id FROM tbl_class WHERE description = %s"
        cursor.execute(obter_id, (class_description,))

        id_registro_tbl_class = cursor.fetchone()[0]
    
    #tbl_category
    verificar_registro = "SELECT COUNT(*) FROM tbl_category WHERE description = %s"
    cursor.execute(verificar_registro, (category,))

    retorno_consulta = cursor.fetchone()

    if retorno_consulta[0] == 0:
        registrar_category = "INSERT INTO tbl_category (description) VALUES (%s) RETURNING ID"
        cursor.execute(registrar_category, (category,))
        id_registro_tbl_category = cursor.fetchone()[0]
    else:
        obter_id = "SELECT id FROM tbl_category WHERE description = %s"
        cursor.execute(obter_id, (category,))

        id_registro_tbl_category = cursor.fetchone()[0]

    #tbl_nominees
    verificar_registro = "SELECT COUNT(*) FROM tbl_nominees WHERE oscar_id = %s AND movie_id = %s AND category_id = %s AND class_id = %s"
    cursor.execute(verificar_registro, (id_registro_tbl_oscar,id_registro_tbl_movie,id_registro_tbl_category,id_registro_tbl_class,))

    retorno_consulta = cursor.fetchone()

    if retorno_consulta[0] == 0:
        registrar_nominees = "INSERT INTO tbl_nominees (oscar_id, class_id, category_id, movie_id, name, nominees, winner, detail, note) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(registrar_nominees, (id_registro_tbl_oscar, id_registro_tbl_class, id_registro_tbl_category, id_registro_tbl_movie, name, nominees, winner, detail, note,))
    else:
        raise ValueError("Nomeação já inserida anteriormente")

#contador de registros
cont = 2

#iterando sobre as linhas da tabela a ser importada
for linha in arquivo.itertuples(index=False):
    ceremony = linha.CEREMONY #okay
    year = linha.YEAR #okay
    class_description = linha.CLASS #okay
    category = linha.CATEGORY #okay
    movie = linha.MOVIE #okay
    name = linha.NAME #okay
    nominees = linha.NOMINEES #okay
    winner = linha.WINNER #okay
    detail = linha.DETAIL #okay
    note = linha.NOTE #okay

    #validador_tbl_class
    category = validador_tbl_category(category)

    if category == None:
        dados_nao_import.append([cont, "Categoria não encontrada"])
        cont += 1
        continue

    #validador_tbl_movie
    movie = validador_tbl_movie(movie, category)

    if movie == False:
        dados_nao_import.append([cont, f"Filme não informado para categoria que o exige: {category}"])
        cont += 1
        continue

    #validador do nome de pessoas
    name = validador_name(name, category, nominees)
    
    if name == False:
        dados_nao_import.append([cont, f"Pessoa não informada para categoria que o exige: {category}"])
        cont += 1
        continue

    nominees = validar_nominees(nominees, category, name)
    if nominees == False:
        dados_nao_import.append([cont, f"Nomeado não informado para categoria que o exige: {category}"])
        cont += 1
        continue

    #validador_tlb_oscar
    year, ceremony = validador_tlb_oscar(year, ceremony)

    if year == False and ceremony == False:
        dados_nao_import.append([cont, f"Ano e cerimônia não informados"])
        cont += 1
        continue

    #validador_tbl_class
    class_description = validador_tbl_class(class_description)

    if class_description == False:
        dados_nao_import.append([cont, f"Classe não encontrada"])
        cont += 1
        continue

    winner, detail, note = validador_tbl_nominees(winner, detail, note)

    if winner == "erro":
        dados_nao_import.append([cont, f"Não informado se venceu ou não corretamente"])
        cont += 1
        continue

    try:
        importador(movie, year, ceremony, class_description, category, name, nominees, winner, detail, note)

        qtd_dados_import += 1
    except Exception as e:
        dados_nao_import.append([cont, f"Erro ao importar registro: {e}"])
        cont += 1
        continue
    
conexao.commit()
cursor.close()
conexao.close()

horario_atual = datetime.now()
duracao_import = str(datetime.now() - horario_inicio).split(".")[0]
print("Duração da importação:", duracao_import)        
print("Total de registros importados:", qtd_dados_import)

print("\nTotal de registros não importados:",len(dados_nao_import))
print("\nRegistros não importados:\n")

for rl in dados_nao_import:
    print("Registro:", rl[0])
    print("Motivo:", rl[1],"\n")
