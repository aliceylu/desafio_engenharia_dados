"""
Gerador de massa fictícia - Case Pleno NovaRota (Pipeline Incremental com Delta Lake)

Gera 5 tipos de arquivo CSV com problemas de qualidade INTENCIONAIS:
  - clientes.csv
  - contas.csv
  - cartoes.csv
  - transacoes_cartao_YYYYMMDD.csv (3 dias de carga)
  - estornos.csv

Uso:
    python gerar_massa.py                       # grava em ./data/raw
    python gerar_massa.py --saida /Volumes/catalogo/schema/raw --inicio 2025-03-01 --dias 3

Dados 100% fictícios (seed fixa => resultado reproduzível).
"""
import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42

CIDADES = [("Porto Alegre", "RS"), ("Florianopolis", "SC"), ("Curitiba", "PR"),
           ("Sao Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
           ("Brasilia", "DF"), ("Salvador", "BA")]
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fabio", "Gabriela", "Hugo", "Isabela",
         "Joao", "Karina", "Lucas", "Marina", "Nicolas", "Olivia", "Paulo", "Quiteria",
         "Rafael", "Sofia", "Tiago"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Pereira", "Lima", "Costa", "Ribeiro",
              "Almeida", "Gomes"]
ESTABELECIMENTOS = [("Mercado Bom Preco", 5411), ("Posto Estrada Azul", 5541),
                    ("Farmacia Saude Total", 5912), ("Restaurante Sabor Caseiro", 5812),
                    ("Loja Moda Facil", 5651), ("Livraria Pagina Nova", 5942),
                    ("Eletro Center", 5732), ("Cafe do Centro", 5814),
                    ("Hotel Vista Mar", 7011), ("Aplicativo Transporte X", 4121)]
BANDEIRAS = ["VISA", "MASTERCARD", "ELO"]


# ----------------------------------------------------------------------------
# Utilitários
# ----------------------------------------------------------------------------
def gerar_cpf_valido(rng: random.Random) -> str:
    nums = [rng.randint(0, 9) for _ in range(9)]
    for _ in range(2):
        peso = len(nums) + 1
        soma = sum(n * (peso - i) for i, n in enumerate(nums))
        dv = (soma * 10 % 11) % 10
        nums.append(dv)
    return "".join(map(str, nums))


def cpf_invalido(rng: random.Random, tipo: int) -> str:
    if tipo == 0:
        return "11111111111"                       # dígitos repetidos
    if tipo == 1:
        return gerar_cpf_valido(rng)[:9]           # tamanho errado
    if tipo == 2:
        c = gerar_cpf_valido(rng)                  # dígito verificador errado
        return c[:-1] + str((int(c[-1]) + 1) % 10)
    return "ABC12345678"                           # caracteres inválidos


def escrever_csv(caminho: Path, cabecalho: list, linhas: list) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cabecalho)
        w.writerows(linhas)
    print(f"[ok] {caminho} ({len(linhas)} linhas)")


# ----------------------------------------------------------------------------
# Geradores
# ----------------------------------------------------------------------------
def gerar_clientes(rng, base: date, n=30):
    linhas, ids = [], []
    for i in range(1, n + 1):
        id_cliente = f"CLI{i:04d}"
        ids.append(id_cliente)
        cidade, uf = rng.choice(CIDADES)
        nasc = date(1950, 1, 1) + timedelta(days=rng.randint(0, 20000))
        atual = datetime.combine(base - timedelta(days=rng.randint(30, 90)), datetime.min.time())
        cpf = gerar_cpf_valido(rng)
        # PROBLEMA: CPFs inválidos (clientes 3, 7, 11, 19)
        if i in (3, 7, 11, 19):
            cpf = cpf_invalido(rng, i % 4)
        nome = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
        linhas.append([id_cliente, nome, cpf, nasc.isoformat(), cidade, uf, atual.isoformat(sep=" ")])

    # PROBLEMA: clientes duplicados com versões diferentes (cidade/nome mudam, data_atualizacao mais nova)
    for idx in (1, 5, 9, 14):
        orig = linhas[idx]
        cidade, uf = rng.choice([c for c in CIDADES if c[0] != orig[4]])
        nova_data = datetime.fromisoformat(orig[6]) + timedelta(days=rng.randint(5, 30))
        linhas.append([orig[0], orig[1] + " (atualizado)", orig[2], orig[3], cidade, uf,
                       nova_data.isoformat(sep=" ")])
    # PROBLEMA: duplicata exata (mesma versão, mesma data) - testa desempate/hash_linha
    linhas.append(list(linhas[2]))
    # PROBLEMA: data_nascimento inválida
    linhas[20][3] = "1990-02-30"
    rng.shuffle(linhas)
    return linhas, ids


def gerar_contas(rng, base: date, ids_clientes, n=35):
    linhas, ids = [], []
    tipos = ["CORRENTE", "POUPANCA"]
    for i in range(1, n + 1):
        id_conta = f"CTA{i:04d}"
        ids.append(id_conta)
        id_cli = rng.choice(ids_clientes)
        status = rng.choice(["ATIVA", "ATIVA", "ATIVA", "BLOQUEADA", "ENCERRADA"])
        abertura = base - timedelta(days=rng.randint(100, 2000))
        # PROBLEMA: conta sem cliente (cliente inexistente)
        if i in (4, 12, 25):
            id_cli = f"CLI9{i:03d}"
        # PROBLEMA: status inconsistente (valor fora do domínio / caixa diferente / nulo)
        if i == 6:
            status = "ativa"
        if i == 15:
            status = "XPTO"
        if i == 22:
            status = ""
        # PROBLEMA: data_abertura no futuro
        if i == 30:
            abertura = base + timedelta(days=400)
        linhas.append([id_conta, id_cli, rng.choice(tipos), status, abertura.isoformat()])
    return linhas, ids


def gerar_cartoes(rng, base: date, ids_contas, n=45):
    linhas, ids = [], []
    for i in range(1, n + 1):
        id_cartao = f"CAR{i:04d}"
        ids.append(id_cartao)
        id_conta = rng.choice(ids_contas)
        status = rng.choice(["ATIVO", "ATIVO", "ATIVO", "BLOQUEADO"])
        emissao = base - timedelta(days=rng.randint(60, 900))
        atual = datetime.combine(emissao + timedelta(days=rng.randint(0, 50)), datetime.min.time())
        # PROBLEMA: cartões cancelados
        if i % 9 == 0:
            status = "CANCELADO"
        # PROBLEMA: cartões sem conta válida
        if i in (5, 17, 33):
            id_conta = f"CTA9{i:03d}"
        linhas.append([id_cartao, id_conta, rng.choice(BANDEIRAS), rng.choice(["CREDITO", "DEBITO"]),
                       status, emissao.isoformat(), atual.isoformat(sep=" ")])

    # PROBLEMA: versões duplicadas de cartão (ex.: virou CANCELADO depois)
    for idx in (0, 7, 20):
        orig = linhas[idx]
        nova = datetime.fromisoformat(orig[6]) + timedelta(days=rng.randint(10, 60))
        linhas.append([orig[0], orig[1], orig[2], orig[3], "CANCELADO", orig[5], nova.isoformat(sep=" ")])
    rng.shuffle(linhas)
    return linhas, ids


def gerar_transacoes(rng, inicio: date, dias: int, ids_cartoes, saida: Path):
    cab = ["id_transacao", "id_cartao", "data_transacao", "valor",
           "nome_estabelecimento", "mcc", "cidade_estabelecimento"]
    seq = 1
    todas = {}          # id_transacao -> linha (para duplicidade entre arquivos e estornos)
    ids_validos = []

    for d in range(dias):
        dia = inicio + timedelta(days=d)
        linhas = []
        for _ in range(60):
            id_trx = f"TRX{seq:06d}"
            seq += 1
            est, mcc = rng.choice(ESTABELECIMENTOS)
            cidade, _uf = rng.choice(CIDADES)
            hora = datetime.combine(dia, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
            linha = [id_trx, rng.choice(ids_cartoes), hora.isoformat(sep=" "),
                     f"{rng.uniform(5, 1500):.2f}", est, mcc, cidade]
            linhas.append(linha)
            todas[id_trx] = linha
            ids_validos.append(id_trx)

        # PROBLEMA: duplicadas dentro do mesmo arquivo
        linhas += [list(l) for l in rng.sample(linhas, 4)]
        # PROBLEMA: valores negativos
        for l in rng.sample(linhas[:60], 3):
            l[3] = f"-{abs(float(l[3])):.2f}"
        # PROBLEMA: valor zero / nulo
        linhas[10][3] = "0.00"
        linhas[11][3] = ""
        # PROBLEMA: datas inválidas
        linhas[20][2] = "2025-13-45 10:00:00"
        linhas[21][2] = "31/02/2025"
        linhas[22][2] = ""
        # PROBLEMA: cartão inexistente (integridade referencial)
        linhas[30][1] = "CAR9999"
        # PROBLEMA: registros atrasados (data_transacao de dias anteriores chegando só neste arquivo)
        if d > 0:
            for k in range(5):
                atraso = timedelta(days=rng.randint(1, d), seconds=rng.randint(0, 80000))
                hora = datetime.combine(dia, datetime.min.time()) - atraso
                est, mcc = rng.choice(ESTABELECIMENTOS)
                id_trx = f"TRX{seq:06d}"
                seq += 1
                l = [id_trx, rng.choice(ids_cartoes), hora.isoformat(sep=" "),
                     f"{rng.uniform(5, 800):.2f}", est, mcc, rng.choice(CIDADES)[0]]
                linhas.append(l)
                todas[id_trx] = l
                ids_validos.append(id_trx)
            # PROBLEMA: reenvio de transação do dia anterior (duplicidade ENTRE arquivos),
            # inclusive com valor corrigido (versão diferente)
            reenvio = list(rng.choice(list(todas.values())[: (d * 60)]))
            reenvio[3] = f"{float(reenvio[3]) + 1.5:.2f}" if reenvio[3] not in ("", "0.00") and not reenvio[3].startswith("-") else reenvio[3]
            linhas.append(reenvio)

        rng.shuffle(linhas)
        escrever_csv(saida / f"transacoes_cartao_{dia.strftime('%Y%m%d')}.csv", cab, linhas)
    return ids_validos


def gerar_estornos(rng, inicio: date, dias: int, ids_trx, n=15):
    linhas = []
    motivos = ["FRAUDE", "COMPRA_NAO_RECONHECIDA", "PRODUTO_NAO_ENTREGUE", "VALOR_INCORRETO", "CANCELAMENTO"]
    for i in range(1, n + 1):
        # PROBLEMA: estornos para transações inexistentes (3 últimos)
        id_trx = rng.choice(ids_trx) if i <= n - 3 else f"TRX9{i:05d}"
        data_est = inicio + timedelta(days=rng.randint(0, dias - 1))
        linhas.append([f"EST{i:04d}", id_trx, data_est.isoformat(), rng.choice(motivos)])
    # PROBLEMA: estorno duplicado para a mesma transação
    linhas.append([f"EST{n + 1:04d}", linhas[0][1], linhas[0][2], "CANCELAMENTO"])
    return linhas


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Gera massa fictícia do case NovaRota")
    ap.add_argument("--saida", default="data/raw", help="diretório de saída")
    ap.add_argument("--inicio", default="2025-03-01", help="primeira data de carga (YYYY-MM-DD)")
    ap.add_argument("--dias", type=int, default=3, help="quantidade de dias de carga (>=3)")
    args = ap.parse_args()

    rng = random.Random(SEED)
    saida = Path(args.saida)
    inicio = date.fromisoformat(args.inicio)

    clientes, ids_cli = gerar_clientes(rng, inicio)
    contas, ids_cta = gerar_contas(rng, inicio, ids_cli)
    cartoes, ids_car = gerar_cartoes(rng, inicio, ids_cta)

    escrever_csv(saida / "clientes.csv",
                 ["id_cliente", "nome", "cpf", "data_nascimento", "cidade", "estado", "data_atualizacao"], clientes)
    escrever_csv(saida / "contas.csv",
                 ["id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura"], contas)
    escrever_csv(saida / "cartoes.csv",
                 ["id_cartao", "id_conta", "bandeira", "tipo_cartao", "status_cartao",
                  "data_emissao", "data_atualizacao"], cartoes)
    ids_trx = gerar_transacoes(rng, inicio, args.dias, ids_car, saida)
    estornos = gerar_estornos(rng, inicio, args.dias, ids_trx)
    escrever_csv(saida / "estornos.csv",
                 ["id_estorno", "id_transacao", "data_estorno", "motivo_estorno"], estornos)


if __name__ == "__main__":
    main()
