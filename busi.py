import fdb 
import pandas as pd
import matplotlib.pyplot as plot
from mlxtend.frequent_patterns import apriori, association_rules

try:
    conexao = fdb.connect(
        dsn = 'localhost:D:\\trabalho\\trabalho_bi\\banco.fdb',
        user = 'SYSDBA',
        password = 'masterkey',
        fb_library_name = 'D:\\trabalho\\trabalho_bi\\Firebird-5.0.1.1469-0-windows-x64\\fbclient.dll'
    )
except Exception as e:
    print(f'Não foi possível conectar ao banco de dados. "{e}"')

try:
    query = """
SELECT DISTINCT 
    PED.PEDIDO_ID,
    PED.DATA,
    CLI.CLIENTE_ID,
    CLI.NOME,
    PED.VALOR_TOTAL,
    ITPED.PEDIDO_ID,
    PROD.NOME,
    ITPED.QUANTIDADE,
    CLI.BAIRRO,
    CLI.CIDADE,
    CLI.UF
FROM
    PEDIDOS PED
LEFT JOIN
    ITENS_PEDIDOS ITPED ON ITPED.PEDIDO_ID = PED.PEDIDO_ID 
LEFT JOIN 
    PRODUTOS PROD ON ITPED.PRODUTO_ID = PROD.PRODUTO_ID
LEFT JOIN 
    CLIENTES CLI ON PED.CLIENTE_ID = CLI.CLIENTE_ID 
WHERE
    PED.DATA >= DATEADD(-1 YEAR TO CURRENT_DATE)
FETCH FIRST 163 ROWS ONLY
"""
except Exception as e:
    print(f'Não foi possível obter os dados necessários para o Dataframe. "{e}"')
finally:
    print('Obtendo dados do banco de dados...')
    dataframe = pd.read_sql(query, conexao)

    print('Normalizando campo de data...')
    dataframe['DATA'] = pd.to_datetime(dataframe['DATA'])

    conexao.close

dataframe.columns = ['pedido_id', 'data', 'cliente_id', 'cliente', 'valor_compra', 'produto_id', 'produto', 'quantidade', 'bairro', 'cidade', 'uf']

print('Analisando produtos mais vendidos por trimestre...')
# Agrupando os dados por trimestre e produto
dataframe['trimestre'] = dataframe['data'].dt.to_period('Q')  # Agrupando por trimestre
produtos_por_trimestre = dataframe.groupby(['trimestre', 'produto'])['quantidade'].sum().reset_index()

# Pegando os 10 produtos mais vendidos por trimestre
top_produtos_trimestre = produtos_por_trimestre.sort_values(['trimestre', 'quantidade'], ascending=[True, False])

# Gráfico de produtos mais vendidos por trimestre
fig, ax = plot.subplots(figsize=(10, 6))

for trimestre, dados in top_produtos_trimestre.groupby('trimestre'):
    ax.bar(dados['produto'], dados['quantidade'], label=str(trimestre))

ax.set_title('Produtos Mais Vendidos por Trimestre')
ax.set_xlabel('Produto')
ax.set_ylabel('Quantidade Vendida')
ax.legend(title='Trimestre', bbox_to_anchor=(1.05, 1), loc='upper left')
plot.xticks(rotation=90)
plot.tight_layout()
plot.show()

# Salvando o relatório de produtos por trimestre
try:
    produtos_por_trimestre.to_excel('produtos_por_trimestre.xlsx', index=False)
    print('Relatório salvo para produtos_por_trimestre.xlsx')
except Exception as e:
    print(f'Erro ao salvar o relatório de produtos por trimestre: {e}')

print('Analisando volume de vendas por cidade...')
# Análise de volume de vendas por cidade
vendas_por_cidade = dataframe.groupby('cidade')['quantidade'].sum().reset_index()
plot.figure(figsize=(10, 5))
plot.bar(vendas_por_cidade['cidade'], vendas_por_cidade['quantidade'], color='skyblue')
plot.title('Volume de vendas por cidade')
plot.xlabel('Cidade')
plot.ylabel('Volume de vendas')
plot.xticks(rotation=90)
plot.show()

vendas_por_cidade.to_excel('volume_vendas_cidade.xlsx', index=False)
print('Relatório salvo para volume_vendas_cidades.xlsx')

print('Segmentando clientes e classificando...')
# Segmentar clientes para campanhas de marketing
clientes_segmentados = dataframe.groupby('cliente_id')['valor_compra'].sum().reset_index()
clientes_segmentados['segmento'] = pd.cut(clientes_segmentados['valor_compra'], 
                                           bins=[0, clientes_segmentados['valor_compra'].quantile(0.25), 
                                                 clientes_segmentados['valor_compra'].quantile(0.5), 
                                                 clientes_segmentados['valor_compra'].quantile(0.75), 
                                                 clientes_segmentados['valor_compra'].max()], 
                                           labels=['Bronze', 'Prata', 'Ouro', 'Diamante'])
clientes_segmentados['segmento'].value_counts().plot(kind='bar', color='lightgreen')
plot.title('Distribuição de Clientes por Segmento')
plot.xlabel('Segmento')
plot.ylabel('Número de Clientes')
plot.show()

clientes_segmentados.to_excel('segmentacao_clientes.xlsx', index=False)
print('Relatório salvo para segmentacao_clientes.xlsx')

from mlxtend.frequent_patterns import apriori, association_rules

# Agrupar os dados por pedido e produto
cesta = dataframe.groupby(['pedido_id', 'produto'])['quantidade'].sum().unstack().fillna(0)

cesta_filtrada = cesta.loc[:, cesta.sum(axis=0) > 105]

# Criar uma tabela binária de transações
cesta_binaria = cesta_filtrada.applymap(lambda x: True if x > 0 else False)

# Aplicar o algoritmo Apriori
regras = apriori(cesta_binaria, min_support=0.01, use_colnames=True)

# Gerar as regras de associação
regras_associacao = association_rules(regras, metric='lift', min_threshold=1.2, num_itemsets=3)

# Exibindo as 5 primeiras regras
print(regras_associacao[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head())

# Selecionando as top 10 regras com maior lift
top_regras = regras_associacao.nlargest(10, 'lift')
print(top_regras[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head())

# Concatenando antecedente e consequente para exibição
top_regras['regra'] = top_regras['antecedents'].astype(str) + " -> " + top_regras['consequents'].astype(str)

# Gerando gráficos para as métricas de Suporte, Confiança e Lift para as top 10 regras
fig, ax = plot.subplots(1, 3, figsize=(15, 5))

# Gráfico de Suporte
ax[0].bar(top_regras['regra'], top_regras['support'], color='skyblue')
ax[0].set_title('Suporte das Regras')
ax[0].set_xlabel('Regra (Antecedente -> Consequente)')
ax[0].set_ylabel('Suporte')
ax[0].tick_params(axis='x', rotation=90)

# Gráfico de Confiança
ax[1].bar(top_regras['regra'], top_regras['confidence'], color='lightgreen')
ax[1].set_title('Confiança das Regras')
ax[1].set_xlabel('Regra (Antecedente -> Consequente)')
ax[1].set_ylabel('Confiança')
ax[1].tick_params(axis='x', rotation=90)

# Gráfico de Lift
ax[2].bar(top_regras['regra'], top_regras['lift'], color='orange')
ax[2].set_title('Lift das Regras')
ax[2].set_xlabel('Regra (Antecedente -> Consequente)')
ax[2].set_ylabel('Lift')
ax[2].tick_params(axis='x', rotation=90)

plot.tight_layout()
plot.show()

# Salvando as top 10 regras de associação em um relatório Excel
top_regras[['antecedents', 'consequents', 'support', 'confidence', 'lift']].to_excel('relatorio_top_regras_associacao.xlsx', index=False)
print("Relatório das top 10 regras de associação salvo como 'relatorio_top_regras_associacao.xlsx'.")