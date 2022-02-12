import firebirdsql as fdb 
import pandas as ps
import datetime
import requests
from sys import exit



def conn(host,database,port,user,password,charset):
    try:
        print("Conectando ao banco...")

        con = fdb.connect(
            host='localhost', 
            database='/ecosis/dados/ecodados.eco', 
            port=3050,
            user='sysdba',
            password='masterkey',
            charset='ANSI'
        )
        print("Conectado...")
        return con
    except: 
        input('Contate o administrador, conexão com o banco de dados não estabelecida!\nTecle ENTER para sair...')
        exit()





def getProdutos(con):
    query = """
    select
        pg.codigo as "Código do Produto",
        pg.codigobarra as "Código de Barras",
        pg.descricao as "Descrição Produto",
        pg.embalagem as "Embalagem",
        m.codigo as "Marca",
        m.descricao as "Marca Descrição",
        sg.grupo as "Grupo",
        g.descricao as "Grupo Descrição(não alterar)",
        sg.grupo as "SubGrupo",
        sg.descricao as "SubGrupo Descrição(não alterar)",

        p.estreservado as "Estoque Reservado",
        p.estdisponivel as "Estoque Dísponivel",
        p.custofabrica as "Custo de Fábrica", p.custofinal as "Custo de Venda",
        p.ativo as "Ativo(S)/Inativo(N)",
        pg.observacao as "Observações",
        pg.obs as "OBS",

        pg.classificacaofiscal as "NCM",
        pg.ipi as "IPI",
        c.cest as "Cest",


        gic.estado as "ICMS Estado",
        gic.grupoid as "ICMS Grupo",
        gi.descricao as "ICMS Descrição Grupo",


        gic.compracsf as "ICMS Compra CST",
        gic.compraicms as "ICMS Compra Alíq.",
        gic.comprareducao as "ICMS Compra Redução",
        gic.compramargem as "ICMS Compra Margem",
        gic.compraicmssubst as "ICMS Compra Alíq. Sub",
        gic.comprareducaosubst as "ICMS Compra Redução Sub",

        gic.vendacsf1 as "ICMS VendaCons CST",
        gic.vendaicms1 as "ICMS VendaCons Alíq.",
        gic.vendareducao1 as "ICMS VendaCons Redução",
        gic.vendamargem1 as "ICMS VendaCons Margem",
        gic.vendaicmssubst1 as "ICMS VendaCons Alíq. Sub",
        gic.vendareducaosubst1 as "ICMS VendaCons Redução Sub",
        gic.cfop1 as "ICMS VendaCons CFOP",

        gic.vendacsf2 as "ICMS Revenda CST",
        gic.vendaicms2 as "ICMS Revenda Alíq.",
        gic.vendareducao2 as "ICMS Revenda Redução",
        gic.vendamargem2 as "ICMS Revenda Margem",
        gic.vendaicmssubst2 as "ICMS Revenda Alíq. Sub",
        gic.vendareducaosubst2 as "ICMS Revenda Redução Sub",
        gic.cfop2 as "ICMS Revenda CFOP",



        gti.cstpisentrada as "PIS Entrada CST",
        gti.aliqpisentrada as "PIS Entrada Alíq.",
        gti.cstcofinsentrada as "COFINS Entrada CST",
        gti.aliqcofinsentrada as "COFINS Entrada Alíq.",
        gti.cstpissaida as "PIS Saída CST",
        gti.aliqpissaida as "PIS Saída Alíq.",
        gti.cstcofinssaida as "COFINS Saída CST",
        gti.aliqcofinssaida as "COFINS Saída Alíq."

    from testprodutogeral pg

        inner join testproduto p on
            pg.codigo = p.produto

        left join testcest c on
            pg.idcest = c.idcest

        left join testgrupoicms gi on
            pg.grupoicms = gi.codigoid

        left join testgrupoicmscomp gic on
            pg.grupoicms = gic.grupoid and p.estado = gic.estado

        left join testgrupo g on
            p.grupo = g.codigo

        left join testsubgrupo sg on
            p.grupo = sg.grupo and p.subgrupo = sg.subgrupo

        left join testgrupotributacaoitem gti on
            p.idgrupotributacao = gti.idgrupotributacao

        left join testmarca m on
            pg.marca = m.codigo


    where
        p.empresa = 01

    order by pg.codigo
    """

    print("Rodando pesquisa...")

    cur = con.cursor()
    cur.execute(query)

    print("Pesquisa concluída...")
    print("Gerando relatório de produtos... aguarde!")

    d = {
        "Código do Produto": [],
        "Código de Barras": [],
        "Descrição Produto": [],
        "Embalagem": [],
        "Marca": [],
        "Marca Descrição": [],
        "Grupo": [],
        "Grupo Descrição(não alterar)": [],
        "SubGrupo": [],
        "SubGrupo Descrição(não alterar)": [],

        "Estoque Reservado": [],
        "Estoque Dísponivel": [],
        "Custo de Fábrica": [], 
        "Custo de Venda": [],
        "Ativo(S)/Inativo(N)": [],
        "Observações": [],
        "OBS": [],

        "NCM": [],
        "IPI": [],
        "Cest": [],


        "ICMS Estado": [],
        "ICMS Grupo": [],
        "ICMS Descrição Grupo": [],


        "ICMS Compra CST": [],
        "ICMS Compra Alíq.": [],
        "ICMS Compra Redução": [],
        "ICMS Compra Margem": [],
        "ICMS Compra Alíq. Sub": [],
        "ICMS Compra Redução Sub": [],

        "ICMS VendaCons CST": [],
        "ICMS VendaCons Alíq.": [],
        "ICMS VendaCons Redução": [],
        "ICMS VendaCons Margem": [],
        "ICMS VendaCons Alíq. Sub": [],
        "ICMS VendaCons Redução Sub": [],
        "ICMS VendaCons CFOP": [],

        "ICMS Revenda CST": [],
        "ICMS Revenda Alíq.": [],
        "ICMS Revenda Redução": [],
        "ICMS Revenda Margem": [],
        "ICMS Revenda Alíq. Sub": [],
        "ICMS Revenda Redução Sub": [],
        "ICMS Revenda CFOP": [],



        "PIS Entrada CST": [],
        "PIS Entrada Alíq.": [],
        "COFINS Entrada CST": [],
        "COFINS Entrada Alíq.": [],
        "PIS Saída CST": [],
        "PIS Saída Alíq.": [],
        "COFINS Saída CST": [],
        "COFINS Saída Alíq.": [],
    }
    i = 0

    for c in cur.fetchall():
        d["Código do Produto"].append(c[i])
        i = i + 1
        d["Código de Barras"].append(c[i])
        i = i + 1
        d["Descrição Produto"].append(c[i])
        i = i + 1
        d["Embalagem"].append(c[i])
        i = i + 1
        d["Marca"].append(c[i])
        i = i + 1
        d["Marca Descrição"].append(c[i])
        i = i + 1
        d["Grupo"].append(c[i])
        i = i + 1
        d["Grupo Descrição(não alterar)"].append(c[i])
        i = i + 1
        d["SubGrupo"].append(c[i])
        i = i + 1
        d["SubGrupo Descrição(não alterar)"].append(c[i])
        i = i + 1
        d["Estoque Reservado"].append(c[i])
        i = i + 1
        d["Estoque Dísponivel"].append(c[i])
        i = i + 1
        d["Custo de Fábrica"].append(c[i])
        i = i + 1
        d["Custo de Venda"].append(c[i])
        i = i + 1
        d["Ativo(S)/Inativo(N)"].append(c[i])
        i = i + 1
        d["Observações"].append(c[i])
        i = i + 1
        d["OBS"].append(c[i])
        i = i + 1
        d["NCM"].append(c[i])
        i = i + 1
        d["IPI"].append(c[i])
        i = i + 1
        d["Cest"].append(c[i])
        i = i + 1
        d["ICMS Estado"].append(c[i])
        i = i + 1
        d["ICMS Grupo"].append(c[i])
        i = i + 1
        d["ICMS Descrição Grupo"].append(c[i])
        i = i + 1
        d["ICMS Compra CST"].append(c[i])
        i = i + 1
        d["ICMS Compra Alíq."].append(c[i])
        i = i + 1
        d["ICMS Compra Redução"].append(c[i])
        i = i + 1
        d["ICMS Compra Margem"].append(c[i])
        i = i + 1
        d["ICMS Compra Alíq. Sub"].append(c[i])
        i = i + 1
        d["ICMS Compra Redução Sub"].append(c[i])
        i = i + 1
        d["ICMS VendaCons CST"].append(c[i])
        i = i + 1
        d["ICMS VendaCons Alíq."].append(c[i])
        i = i + 1
        d["ICMS VendaCons Redução"].append(c[i])
        i = i + 1
        d["ICMS VendaCons Margem"].append(c[i])
        i = i + 1
        d["ICMS VendaCons Alíq. Sub"].append(c[i])
        i = i + 1
        d["ICMS VendaCons Redução Sub"].append(c[i])
        i = i + 1
        d["ICMS VendaCons CFOP"].append(c[i])
        i = i + 1
        d["ICMS Revenda CST"].append(c[i])
        i = i + 1
        d["ICMS Revenda Alíq."].append(c[i])
        i = i + 1
        d["ICMS Revenda Redução"].append(c[i])
        i = i + 1
        d["ICMS Revenda Margem"].append(c[i])
        i = i + 1
        d["ICMS Revenda Alíq. Sub"].append(c[i])
        i = i + 1
        d["ICMS Revenda Redução Sub"].append(c[i])
        i = i + 1
        d["ICMS Revenda CFOP"].append(c[i])
        i = i + 1
        d["PIS Entrada CST"].append(c[i])
        i = i + 1
        d["PIS Entrada Alíq."].append(c[i])
        i = i + 1
        d["COFINS Entrada CST"].append(c[i])
        i = i + 1
        d["COFINS Entrada Alíq."].append(c[i])
        i = i + 1
        d["PIS Saída CST"].append(c[i])
        i = i + 1
        d["PIS Saída Alíq."].append(c[i])
        i = i + 1
        d["COFINS Saída CST"].append(c[i])
        i = i + 1
        d["COFINS Saída Alíq."].append(c[i])
        i = i + 1

        i = 0

    tabela = ps.DataFrame(data=d)
    tabela.to_excel('dados_produtos.xlsx')





def getClientes(con):
    query_cliente = """
    select
        rcg.pessoa as "Tipo Cliente",
        rcg.cpfcnpj as "CPF/CNPJ",
        rcg.rgie as "RG/IE",
        rcg.nome as "Razão Social",
        rcg.fantasia as "Nome Fantasia",
        rcg.endereco as "Endereço",
        rcg.numeroendereco as "Número",
        rcg.bairro as "Bairro",
        rcg.complemento as "Complemento",
        rcg.cep as "CEP",
        rcg.cidade as "Cidade Código",
        c.nome as "Cidade Nome",
        c.codigoibge as "Cidade Cód. IBGE",
        c.estado as "Estado",
        rcg.fone as "Telefone",
        rcg.fonecelular as "Celular",
        rcg.fax as "Fax",
        rcg.email as "Email",
        rcg.obs as "Observações",
        rcg.bloqueado as "Ativo/Inativo"

    from trecclientegeral rcg

        left join tgercidade c on
            rcg.cidade = c.codigo

        left join treccliente rc on
            rcg.codigo = rc.codigo

    where
        rc.empresa = 01

    order by
        rcg.codigo
    """

    print("Rodando pesquisa...")

    cur = con.cursor()
    cur.execute(query_cliente)

    print("Pesquisa concluída...")
    print("Gerando relatório de clientes... aguarde!")

    d = {
        "Tipo Cliente": [],
        "CPF/CNPJ": [],
        "RG/IE": [],
        "Razão Social": [],
        "Nome Fantasia": [],
        "Endereço": [],
        "Número": [],
        "Bairro": [],
        "Complemento": [],
        "CEP": [],
        "Cidade Código": [],
        "Cidade Nome": [],
        "Cidade Cód. IBGE": [],
        "Estado": [],
        "Telefone": [],
        "Celular": [],
        "Fax": [],
        "Email": [],
        "Observações": [],
        "Ativo/Inativo": [],
    }

    len_query = len(cur.fetchone())
    i = 0
    
    for c in cur.fetchall():
        d["Tipo Cliente"].append(c[i])
        i = i + 1
        d["CPF/CNPJ"].append(c[i])
        i = i + 1
        d["RG/IE"].append(c[i])
        i = i + 1
        d["Razão Social"].append(c[i])
        i = i + 1
        d["Nome Fantasia"].append(c[i])
        i = i + 1
        d["Endereço"].append(c[i])
        i = i + 1
        d["Número"].append(c[i])
        i = i + 1
        d["Bairro"].append(c[i])
        i = i + 1
        d["Complemento"].append(c[i])
        i = i + 1
        d["CEP"].append(c[i])
        i = i + 1
        d["Cidade Código"].append(c[i])
        i = i + 1
        d["Cidade Nome"].append(c[i])
        i = i + 1
        d["Cidade Cód. IBGE"].append(c[i])
        i = i + 1
        d["Estado"].append(c[i])
        i = i + 1
        d["Telefone"].append(c[i])
        i = i + 1
        d["Celular"].append(c[i])
        i = i + 1
        d["Fax"].append(c[i])
        i = i + 1
        d["Email"].append(c[i])
        i = i + 1
        d["Observações"].append(c[i])
        i = i + 1
        d["Ativo/Inativo"].append(c[i])
        i = i + 1

        i = 0

    tabela = ps.DataFrame(data=d)
    tabela.to_excel('dados_clientes.xlsx')




def main(status_code, year, month, day):
    permissao = bool(datetime.date.today() < datetime.date(int(year), int(month), int(day))) 

    if permissao and status_code == 200:
        print("\n-> Chave de uso validada! <-\n")
        con = conn('localhost', '/ecosis/dados/ecodados.eco', 3050, 'sysdba', 'masterkey', 'ANSI')
        getProdutos(con)
        getClientes(con)

        import subprocess as s 
        import os
        s.call("explorer.exe /e, dados_produtos.xlsx")
        s.call("explorer.exe /e, dados_clientes.xlsx")
    else: 
        print("Contate o administrador, Chave de uso expirada!")

if __name__ == "__main__":
    codigo = input('Insira o código de uso: ')
    url = 'https://api-softbr-relatorios.herokuapp.com/?key=%s' % codigo
    r = requests.get(url)
    status_code = r.status_code
    try:
        json = r.json()
    except:
        input('Contate o administrador, Chave de uso inexistente!\n Tecle ENTER para sair...')
        exit()
   
    main(status_code, json['year'], json['month'], json['day'])

    # s.call("explorer.exe /e, %s" % os.getcwd())