import fdb 
from faker import Faker
import random
from datetime import datetime, timedelta

try:
    conexao = fdb.connect(
        dsn = 'localhost:E:\\Projetos\\Trabalho\\banco.fdb',
        user = 'SYSDBA',
        password = 'masterkey'
    )
except Exception as e:
    print(f'Não foi possível conectar ao banco de dados. "{e}"')

fake = Faker('pt_BR')
cursor = conexao.cursor()

def inserir_clientes(quantidade):
    cliente_ids = []
    con = 1
    for _ in range(quantidade):
        print(f'\r{con} - Gerando clientes...', end='')

        nome = fake.name()
        bairro = fake.neighborhood()
        cidade = fake.city()
        uf = fake.state_abbr()

        cursor.execute(
            "INSERT INTO clientes (nome, bairro, cidade, uf) VALUES (?, ?, ?, ?) RETURNING cliente_id", 
            (nome, bairro, cidade, uf)
        )

        cliente_id = cursor.fetchone()[0]
        cliente_ids.append(cliente_id)

        con += 1
    
    conexao.commit()
    return cliente_ids

def inserir_produtos(quantidade):
    con = 1
    produto_ids = []
    for _ in range(quantidade):
        print(f'\r{con} - Gerando produto...', end='')

        nome = fake.word().capitalize()
        valor = round(random.uniform(10.0, 280000.0), 2)

        cursor.execute(
            "INSERT INTO produtos (nome, valor) VALUES (?, ?) RETURNING produto_id", 
            (nome, valor)
        )
        
        produto_id = cursor.fetchone()[0]
        produto_ids.append((produto_id, valor))

        con += 1
    
    conexao.commit()
    return produto_ids

def inserir_pedidos(cliente_ids, quantidade):
    pedido_ids = []
    con = 1
    for _ in range(quantidade):
        print(f'\r{con} - Gerando pedidos...', end='')

        cliente_id = random.choice(cliente_ids)
        data = fake.date_time_between(start_date='-1y', end_date='now')
        valor_total = round(random.uniform(50.0, 700000.0), 2)

        cursor.execute(
            "INSERT INTO pedidos (data, valor_total, cliente_id) VALUES (?, ?, ?) RETURNING pedido_id", 
            (data, valor_total, cliente_id)
        )
        
        pedido_id = cursor.fetchone()[0]
        pedido_ids.append(pedido_id)

        con += 1
    
    conexao.commit()
    return pedido_ids

def escolher_produto_ponderado(produtos):
    # Ordena produtos por valor (do mais barato ao mais caro)
    produtos = sorted(produtos, key=lambda x: x[1])
    
    # Probabilidades ponderadas: produtos mais baratos têm maior chance
    pesos = [1 / (i + 1) for i in range(len(produtos))]
    produto_escolhido = random.choices(produtos, weights=pesos, k=1)[0]
    return produto_escolhido  # Retorna o produto como um par (produto_id, valor)

def inserir_itens_pedidos(pedido_ids, produtos):
    con = 1
    for pedido_id in pedido_ids:
        print(f'\r{con} - Gerando itens de pedidos...', end='')
        num_itens = random.randint(5, 53)  # Número aleatório de itens por pedido (entre 5 e 105)
        total_pedido = 0  # Inicializa a soma do valor total do pedido

        for _ in range(num_itens):
            produto_id, valor = escolher_produto_ponderado(produtos)
            quantidade = random.randint(5, 1304) if produto_id in [p[0] for p in produtos[:3]] else random.randint(1, 41)

            # Calcula o valor do item e acumula no total do pedido
            valor_item = quantidade * valor
            total_pedido += valor_item

            # Insere o item no banco de dados
            cursor.execute(
                "INSERT INTO itens_pedidos (produto_id, quantidade, pedido_id) VALUES (?, ?, ?)", 
                (produto_id, quantidade, pedido_id)
            )

        # Atualiza o valor total do pedido na tabela `pedidos`
        cursor.execute(
            "UPDATE pedidos SET valor_total = ? WHERE pedido_id = ?", 
            (total_pedido, pedido_id)
        )

        con += 1

    conexao.commit()

clientes = inserir_clientes(15000)
produtos = inserir_produtos(19000)
pedidos = inserir_pedidos(clientes, 100000) 
inserir_itens_pedidos(pedidos, produtos)

cursor.close()
conexao.close()

print("\nDados inseridos com sucesso!")