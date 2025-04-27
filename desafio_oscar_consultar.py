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

print("1 - Filmes indicados em cada ano na categoria best picture\n")

consulta = """
SELECT
    m.title,
    c.description,
    o.ceremony,
    n.winner
FROM
    tbl_category c
JOIN
    tbl_nominees n ON n.category_id = c.id
JOIN
    tbl_oscar o ON o.id = n.oscar_id
JOIN
    tbl_movie m ON m.id = n.movie_id
WHERE
    c.description = 'best picture'

"""

cursor.execute(consulta)

resultado = cursor.fetchall()

for i in resultado:
    print("Título:",i[0])
    print("Categoria:",i[1])
    print("Cerimônia:",i[2])
    print("Vencedor?",i[3],"\n")
    

print("\n2 - Categorias que houve indicação para o filme Toy Story 3\n")

consulta = """
SELECT
    m.title,
    n.winner,
    c.description,
    o.year

FROM
    tbl_movie m

JOIN
    tbl_nominees n ON n.movie_id = m.id
JOIN
    tbl_category c ON c.id = n.category_id
JOIN
    tbl_oscar o ON o.id = n.oscar_id

WHERE
    m.title = 'toy story 3'"""

cursor.execute(consulta)

resultado = cursor.fetchall()

for i in resultado:
    print("Título:",i[0])
    print("Categoria:",i[2])
    print("Ano:",i[3])
    print("Vencedor?", "sim" if i[1] == True else "não","\n")

print("\n3 - Atores ou atrizes com mais de 3 indicações em qualquer Oscar\n")

consulta = """
SELECT
    n.nominees, COUNT(*) AS total
FROM
    tbl_nominees n
WHERE n.nominees != 'N/A'
GROUP BY
    n.nominees
HAVING COUNT(*) > 3
ORDER BY total ASC;
"""

cursor.execute(consulta)

resultado = cursor.fetchall()

for i in resultado:
    print("Nomeado:",i[0])
    print("Qtd nomeações:",i[1],"\n")

cursor.close()
conexao.close()

