import os
from datetime import datetime, date, timedelta
from io import BytesIO

from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from werkzeug.utils import secure_filename

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float,
    ForeignKey, func, select, insert, update, delete
)
from sqlalchemy.engine import Engine
import pandas as pd

# --------------------------------------------------------------------
# Configuração de banco: Postgres em produção, SQLite em desenvolvimento
# --------------------------------------------------------------------
# Detecta Postgres (Render) ou cai para SQLite local
raw_db_url = os.environ.get("DATABASE_URL")

if raw_db_url:
    # Render costuma entregar "postgres://", mas o SQLAlchemy quer "postgresql+psycopg2://"
    if raw_db_url.startswith("postgres://"):
        raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    DATABASE_URL = raw_db_url
else:
    DATABASE_URL = "sqlite:////tmp/metrifiy.db"
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "uploads")

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.secret_key = os.environ.get("SECRET_KEY", "metrifypremium-secret")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

engine: Engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

# --------------------------------------------------------------------
# Definição das tabelas
# --------------------------------------------------------------------
produtos = Table(
    "produtos",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("nome", String(255), nullable=False),
    Column("sku", String(100), unique=True),
    Column("custo_unitario", Float, nullable=False, server_default="0"),
    Column("preco_venda_sugerido", Float, nullable=False, server_default="0"),
    Column("estoque_inicial", Integer, nullable=False, server_default="0"),
    Column("estoque_atual", Integer, nullable=False, server_default="0"),
    Column("curva", String(1)),
)

vendas = Table(
    "vendas",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, ForeignKey("produtos.id"), nullable=False),
    Column("data_venda", String(50)),
    Column("quantidade", Integer, nullable=False),
    Column("preco_venda_unitario", Float, nullable=False),
    Column("receita_total", Float, nullable=False),
    Column("custo_total", Float, nullable=False),
    Column("margem_contribuicao", Float, nullable=False),
    Column("origem", String(50)),
    Column("numero_venda_ml", String(100)),
    Column("lote_importacao", String(50)),
)

ajustes_estoque = Table(
    "ajustes_estoque",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, ForeignKey("produtos.id"), nullable=False),
    Column("data_ajuste", String(50)),
    Column("tipo", String(20)),  # entrada, saida
    Column("quantidade", Integer),
    Column("custo_unitario", Float),
    Column("observacao", String(255)),
)

configuracoes = Table(
    "configuracoes",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("imposto_percent", Float, nullable=False, server_default="0"),
    Column("despesas_percent", Float, nullable=False, server_default="0"),
)


def init_db():
    """Cria as tabelas se não existirem e garante 1 linha em configuracoes."""
    metadata.create_all(engine)
    with engine.begin() as conn:
        row = conn.execute(
            select(configuracoes.c.id).limit(1)
        ).first()
        if not row:
            conn.execute(
                insert(configuracoes).values(id=1, imposto_percent=0.0, despesas_percent=0.0)
            )


# --------------------------------------------------------------------
# Utilidades para datas
# --------------------------------------------------------------------
MESES_PT = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}


def parse_data_venda(texto):
    if isinstance(texto, datetime):
        return texto
    if not isinstance(texto, str) or not texto.strip():
        return None
    try:
        partes = texto.split()
        dia = int(partes[0])
        mes_nome = partes[2].lower()
        ano = int(partes[4])
        hora_min = partes[5]
        hora, minuto = hora_min.split(":")
        return datetime(ano, MESES_PT[mes_nome], int(dia), int(hora), int(minuto))
    except Exception:
        # tenta ISO
        try:
            return datetime.fromisoformat(texto)
        except Exception:
            return None


# --------------------------------------------------------------------
# Importação de vendas do Mercado Livre
# --------------------------------------------------------------------
def importar_vendas_ml(caminho_arquivo, engine: Engine):
    lote_id = datetime.now().isoformat(timespec="seconds")

    df = pd.read_excel(
        caminho_arquivo,
        sheet_name="Vendas BR",
        header=5
    )
    if "N.º de venda" not in df.columns:
        raise ValueError("Planilha não está no formato esperado: coluna 'N.º de venda' não encontrada.")

    df = df[df["N.º de venda"].notna()]

    vendas_importadas = 0
    vendas_sem_sku = 0
    vendas_sem_produto = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            sku = str(row.get("SKU") or "").strip()
            titulo = str(row.get("Título do anúncio") or "").strip()

            produto_row = None

            if sku:
                produto_row = conn.execute(
                    select(produtos.c.id, produtos.c.custo_unitario)
                    .where(produtos.c.sku == sku)
                ).mappings().first()
            else:
                # tenta pelo nome do produto = título do anúncio
                if titulo:
                    produto_row = conn.execute(
                        select(produtos.c.id, produtos.c.custo_unitario)
                        .where(produtos.c.nome == titulo)
                    ).mappings().first()

            if not sku and not produto_row:
                vendas_sem_sku += 1
                continue

            if not produto_row:
                vendas_sem_produto += 1
                continue

            produto_id = produto_row["id"]
            custo_unitario = float(produto_row["custo_unitario"] or 0.0)

            data_venda_raw = row.get("Data da venda")
            data_venda = parse_data_venda(data_venda_raw)
            unidades = row.get("Unidades")
            try:
                unidades = int(unidades) if unidades == unidades else 0
            except Exception:
                unidades = 0

            total_brl = row.get("Total (BRL)")
            try:
                receita_total = float(total_brl) if total_brl == total_brl else 0.0
            except Exception:
                receita_total = 0.0

            preco_medio_venda = receita_total / unidades if unidades > 0 else 0.0
            custo_total = custo_unitario * unidades

            # Comissão Mercado Livre a partir da coluna 'Tarifa de venda e impostos (BRL)'
            tarifa = row.get("Tarifa de venda e impostos (BRL)")
            try:
                comissao_ml = float(tarifa) if tarifa == tarifa else 0.0
            except Exception:
                comissao_ml = 0.0
            if comissao_ml < 0:
                comissao_ml = -comissao_ml

            margem_contribuicao = receita_total - custo_total - comissao_ml
            numero_venda_ml = str(row.get("N.º de venda"))

            conn.execute(
                insert(vendas).values(
                    produto_id=produto_id,
                    data_venda=data_venda.isoformat() if data_venda else None,
                    quantidade=unidades,
                    preco_venda_unitario=preco_medio_venda,
                    receita_total=receita_total,
                    custo_total=custo_total,
                    margem_contribuicao=margem_contribuicao,
                    origem="Mercado Livre",
                    numero_venda_ml=numero_venda_ml,
                    lote_importacao=lote_id,
                )
            )

            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(estoque_atual=produtos.c.estoque_atual - unidades)
            )

            vendas_importadas += 1

    return {
        "lote_id": lote_id,
        "vendas_importadas": vendas_importadas,
        "vendas_sem_sku": vendas_sem_sku,
        "vendas_sem_produto": vendas_sem_produto,
    }


# --------------------------------------------------------------------
# Rotas principais
# --------------------------------------------------------------------
@app.route("/")
def dashboard():
    # --- filtro de período ---
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""

    # padrão: mês vigente
    if not data_inicio and not data_fim:
        hoje = date.today()
        inicio_mes = hoje.replace(day=1)
        data_inicio = inicio_mes.isoformat()
        data_fim = hoje.isoformat()

    # cria filtro SQL
    filtro_data = []
    if data_inicio:
        filtro_data.append(vendas.c.data_venda >= data_inicio)
    if data_fim:
        filtro_data.append(vendas.c.data_venda <= data_fim + "T23:59:59")

    with engine.connect() as conn:

        # totais de estoque (não dependem do período)
        total_produtos = conn.execute(
            select(func.count()).select_from(produtos)
        ).scalar_one()

        estoque_total = conn.execute(
            select(func.coalesce(func.sum(produtos.c.estoque_atual), 0))
        ).scalar_one()

        # --- totais filtrados por período ---
        receita_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.receita_total), 0))
            .where(*filtro_data)
        ).scalar_one()

        custo_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.custo_total), 0))
            .where(*filtro_data)
        ).scalar_one()

        margem_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.margem_contribuicao), 0))
            .where(*filtro_data)
        ).scalar_one()

        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

        imposto_percent = float(cfg["imposto_percent"]) if cfg else 0.0
        despesas_percent = float(cfg["despesas_percent"]) if cfg else 0.0

        comissao_total = max(0.0, (receita_total - custo_total) - margem_total)
        imposto_total = receita_total * (imposto_percent / 100.0)
        despesas_total = receita_total * (despesas_percent / 100.0)

        lucro_liquido_total = (
            receita_total
            - custo_total
            - comissao_total
            - imposto_total
            - despesas_total
        )

        receita_liquida_total = receita_total - comissao_total - imposto_total - despesas_total

        margem_liquida_percent = (
            (lucro_liquido_total / receita_total) * 100.0
            if receita_total > 0 else 0.0
        )

        ticket_medio = conn.execute(
            select(func.coalesce(func.avg(vendas.c.preco_venda_unitario), 0))
            .where(*filtro_data)
        ).scalar_one()

        # produto mais vendido no período
        produto_mais_vendido = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.quantidade).label("qtd"))
            .select_from(vendas.join(produtos))
            .where(*filtro_data)
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.quantidade).desc())
            .limit(1)
        ).first()

        produto_maior_lucro = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("lucro"))
            .select_from(vendas.join(produtos))
            .where(*filtro_data)
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).desc())
            .limit(1)
        ).first()

        produto_pior_margem = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("margem"))
            .select_from(vendas.join(produtos))
            .where(*filtro_data)
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).asc())
            .limit(1)
        ).first()

    return render_template(
        "dashboard.html",
        receita_total=receita_total,
        receita_liquida_total=receita_liquida_total,
        lucro_liquido_total=lucro_liquido_total,
        margem_liquida_percent=margem_liquida_percent,
        custo_total=custo_total,
        comissao_total=comissao_total,
        imposto_total=imposto_total,
        despesas_total=despesas_total,
        ticket_medio=ticket_medio,
        total_produtos=total_produtos,
        estoque_total=estoque_total,
        produto_mais_vendido=produto_mais_vendido,
        produto_maior_lucro=produto_maior_lucro,
        produto_pior_margem=produto_pior_margem,
        cfg=cfg,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

# ---------------- PRODUTOS ----------------
@app.route("/produtos")
def lista_produtos():
    with engine.connect() as conn:
        produtos_rows = conn.execute(select(produtos).order_by(produtos.c.nome)).mappings().all()
    return render_template("produtos.html", produtos=produtos_rows)


@app.route("/produtos/novo", methods=["GET", "POST"])
def novo_produto():
    if request.method == "POST":
        nome = request.form["nome"]
        sku = request.form["sku"]
        custo_unitario = float(request.form.get("custo_unitario", 0) or 0)
        preco_venda_sugerido = float(request.form.get("preco_venda_sugerido", 0) or 0)
        estoque_inicial = int(request.form.get("estoque_inicial", 0) or 0)

        with engine.begin() as conn:
            conn.execute(
                insert(produtos).values(
                    nome=nome,
                    sku=sku,
                    custo_unitario=custo_unitario,
                    preco_venda_sugerido=preco_venda_sugerido,
                    estoque_inicial=estoque_inicial,
                    estoque_atual=estoque_inicial,
                )
            )
        flash("Produto cadastrado com sucesso!", "success")
        return redirect(url_for("lista_produtos"))

    return render_template("produto_form.html", produto=None)


@app.route("/produtos/<int:produto_id>/editar", methods=["GET", "POST"])
def editar_produto(produto_id):
    if request.method == "POST":
        nome = request.form["nome"]
        sku = request.form["sku"]
        custo_unitario = float(request.form.get("custo_unitario", 0) or 0)
        preco_venda_sugerido = float(request.form.get("preco_venda_sugerido", 0) or 0)
        estoque_atual = int(request.form.get("estoque_atual", 0) or 0)

        with engine.begin() as conn:
            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(
                    nome=nome,
                    sku=sku,
                    custo_unitario=custo_unitario,
                    preco_venda_sugerido=preco_venda_sugerido,
                    estoque_atual=estoque_atual,
                )
            )
        flash("Produto atualizado!", "success")
        return redirect(url_for("lista_produtos"))

    with engine.connect() as conn:
        produto_row = conn.execute(
            select(produtos).where(produtos.c.id == produto_id)
        ).mappings().first()

    if not produto_row:
        flash("Produto não encontrado.", "danger")
        return redirect(url_for("lista_produtos"))

    return render_template("produto_form.html", produto=produto_row)


@app.route("/produtos/<int:produto_id>/excluir", methods=["POST"])
def excluir_produto(produto_id):
    with engine.begin() as conn:
        conn.execute(delete(produtos).where(produtos.c.id == produto_id))
    flash("Produto excluído.", "success")
    return redirect(url_for("lista_produtos"))


# ---------------- VENDAS ----------------
from flask import request, render_template
from sqlalchemy import select, func
from datetime import date, datetime, timedelta
from math import ceil

VENDAS_POR_PAGINA = 100

@app.route("/vendas")
def lista_vendas():
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""
    page = int(request.args.get("page", 1))

    # =======================
    # PERÍODO PADRÃO: ÚLTIMOS 30 DIAS
    # =======================
    hoje = date.today()
    trinta_dias_atras = hoje - timedelta(days=29)
    default_data_inicio = trinta_dias_atras.isoformat()
    default_data_fim = hoje.isoformat()

    if not data_inicio:
        data_inicio = default_data_inicio
    if not data_fim:
        data_fim = default_data_fim

    with engine.connect() as conn:
        # =======================
        # CONSULTA VENDAS (RESPEITA FILTRO DA TELA)
        # =======================
        query_vendas = select(
            vendas.c.id,
            vendas.c.data_venda,
            vendas.c.quantidade,
            vendas.c.preco_venda_unitario,
            vendas.c.receita_total,
            vendas.c.custo_total,
            vendas.c.margem_contribuicao,
            vendas.c.origem,
            vendas.c.numero_venda_ml,
            vendas.c.lote_importacao,
            produtos.c.nome,
        ).select_from(vendas.join(produtos))

        query_vendas = query_vendas.where(
            vendas.c.data_venda >= data_inicio,
            vendas.c.data_venda <= data_fim + "T23:59:59"
        ).order_by(vendas.c.data_venda.asc())

        vendas_all = conn.execute(query_vendas).mappings().all()

        # Paginação
        total_vendas = len(vendas_all)
        total_pages = ceil(total_vendas / VENDAS_POR_PAGINA)
        start = (page - 1) * VENDAS_POR_PAGINA
        end = start + VENDAS_POR_PAGINA
        vendas_rows = vendas_all[start:end]

        # =======================
        # CONSULTA LOTES (RESPEITA FILTRO)
        # =======================
        query_lotes = select(
            vendas.c.lote_importacao.label("lote_importacao"),
            func.count().label("qtd_vendas"),
            func.coalesce(func.sum(vendas.c.receita_total), 0).label("receita_lote"),
        ).where(
            vendas.c.lote_importacao.isnot(None),
            vendas.c.data_venda >= data_inicio,
            vendas.c.data_venda <= data_fim + "T23:59:59"
        ).group_by(vendas.c.lote_importacao)

        lotes = conn.execute(query_lotes).mappings().all()

        # Produtos (pra venda manual, etc.)
        produtos_rows = conn.execute(
            select(produtos.c.id, produtos.c.nome).order_by(produtos.c.nome)
        ).mappings().all()

    # =======================
    # GRÁFICOS 30 DIAS (FATURAMENTO / QTD / LUCRO)
    # =======================
    faturamento_dia = {}
    quantidade_dia = {}
    lucro_dia = {}

    for v in vendas_all:
        if not v["data_venda"]:
            continue
        try:
            dt = datetime.fromisoformat(v["data_venda"]).date()
        except Exception:
            continue

        receita = float(v["receita_total"] or 0)
        custo = float(v["custo_total"] or 0)
        margem = float(v["margem_contribuicao"] or 0)
        qtd = float(v["quantidade"] or 0)

        # lucro líquido do dia (mesma lógica do dashboard)
        comissao_ml = max(0.0, (receita - custo) - margem)
        lucro = receita - custo - comissao_ml

        faturamento_dia[dt] = faturamento_dia.get(dt, 0) + receita
        quantidade_dia[dt] = quantidade_dia.get(dt, 0) + qtd
        lucro_dia[dt] = lucro_dia.get(dt, 0) + lucro

    # Últimos 30 dias ordenados
    dias = [hoje - timedelta(days=i) for i in range(29, -1, -1)]
    grafico_labels = [d.isoformat() for d in dias]
    grafico_faturamento = [faturamento_dia.get(d, 0) for d in dias]
    grafico_quantidade = [quantidade_dia.get(d, 0) for d in dias]
    grafico_lucro = [lucro_dia.get(d, 0) for d in dias]

    # =========================
    # COMPARATIVO MÊS ATUAL x MÊS ANTERIOR
    # (IGNORA O FILTRO DA TELA E BUSCA DIRETO NO BANCO)
    # =========================
    # Início do mês atual
    inicio_mes_atual = hoje.replace(day=1)

    # Mês anterior
    if inicio_mes_atual.month == 1:
        ano_ant = inicio_mes_atual.year - 1
        mes_ant = 12
    else:
        ano_ant = inicio_mes_atual.year
        mes_ant = inicio_mes_atual.month - 1
    inicio_mes_anterior = date(ano_ant, mes_ant, 1)

    # Último dia do mês atual (até hoje)
    if inicio_mes_atual.month == 12:
        primeiro_prox_mes_atual = date(inicio_mes_atual.year + 1, 1, 1)
    else:
        primeiro_prox_mes_atual = date(inicio_mes_atual.year, inicio_mes_atual.month + 1, 1)
    fim_mes_atual = min(hoje, primeiro_prox_mes_atual - timedelta(days=1))

    # Último dia do mês anterior
    if mes_ant == 12:
        primeiro_mes_pos_ant = date(ano_ant + 1, 1, 1)
    else:
        primeiro_mes_pos_ant = date(ano_ant, mes_ant + 1, 1)
    fim_mes_anterior = primeiro_mes_pos_ant - timedelta(days=1)

    # Busca todas as vendas dos dois meses
    with engine.connect() as conn_cmp:
        rows_cmp = conn_cmp.execute(
            select(
                vendas.c.data_venda,
                vendas.c.receita_total
            ).where(
                vendas.c.data_venda >= inicio_mes_anterior.isoformat(),
                vendas.c.data_venda <= fim_mes_atual.isoformat() + "T23:59:59"
            )
        ).mappings().all()

    faturamento_mes_atual = {}
    faturamento_mes_anterior = {}

    for v in rows_cmp:
        data_raw = v["data_venda"]
        if not data_raw:
            continue
        try:
            dt = datetime.fromisoformat(str(data_raw)).date()
        except Exception:
            dt_parsed = parse_data_venda(data_raw)
            if not dt_parsed:
                continue
            dt = dt_parsed.date()

        receita = float(v["receita_total"] or 0)

        if inicio_mes_atual <= dt <= fim_mes_atual:
            faturamento_mes_atual[dt] = faturamento_mes_atual.get(dt, 0) + receita
        elif inicio_mes_anterior <= dt <= fim_mes_anterior:
            faturamento_mes_anterior[dt] = faturamento_mes_anterior.get(dt, 0) + receita

    # Monta lista de dias do mês atual (1 até hoje/último dia)
    dias_mes_atual = []
    d = inicio_mes_atual
    while d <= fim_mes_atual:
        dias_mes_atual.append(d)
        d += timedelta(days=1)

    grafico_cmp_labels = [d.isoformat() for d in dias_mes_atual]
    grafico_cmp_atual = [faturamento_mes_atual.get(d, 0) for d in dias_mes_atual]

    # Para cada dia do mês atual, pegamos o dia equivalente do mês anterior (1 com 1, 2 com 2...)
    grafico_cmp_anterior = []
    for i, _d in enumerate(dias_mes_atual):
        dia_ant = inicio_mes_anterior + timedelta(days=i)
        grafico_cmp_anterior.append(faturamento_mes_anterior.get(dia_ant, 0))

    # =========================
    # TOTAIS (RESPEITAM O FILTRO DA TELA)
    # =========================
    totais = {
        "qtd": sum(q["quantidade"] for q in vendas_all),
        "receita": sum(q["receita_total"] for q in vendas_all),
        "custo": sum(q["custo_total"] for q in vendas_all),
    }

    return render_template(
        "vendas.html",
        vendas=vendas_rows,
        lotes=lotes,
        produtos=produtos_rows,
        data_inicio=data_inicio,
        data_fim=data_fim,
        totais=totais,
        grafico_labels=grafico_labels,
        grafico_faturamento=grafico_faturamento,
        grafico_quantidade=grafico_quantidade,
        grafico_lucro=grafico_lucro,
        grafico_cmp_labels=grafico_cmp_labels,
        grafico_cmp_atual=grafico_cmp_atual,
        grafico_cmp_anterior=grafico_cmp_anterior,
        total_pages=total_pages,
        current_page=page
    )


# ---------------- IMPORT / EXPORT ----------------
@app.route("/importar_ml", methods=["GET", "POST"])
def importar_ml_view():
    if request.method == "POST":
        if "arquivo" not in request.files:
            flash("Nenhum arquivo enviado.", "danger")
            return redirect(request.url)
        file = request.files["arquivo"]
        if file.filename == "":
            flash("Selecione um arquivo.", "danger")
            return redirect(request.url)
        filename = secure_filename(file.filename)
        caminho = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(caminho)

        try:
            resumo = importar_vendas_ml(caminho, engine)
            flash(
                f"Importação concluída. Lote {resumo['lote_id']} - "
                f"{resumo['vendas_importadas']} vendas importadas, "
                f"{resumo['vendas_sem_sku']} sem SKU/Título, "
                f"{resumo['vendas_sem_produto']} sem produto cadastrado.",
                "success",
            )
        except Exception as e:
            flash(f"Erro na importação: {e}", "danger")
        return redirect(url_for("importar_ml_view"))

    return render_template("importar_ml.html")


@app.route("/exportar_consolidado")
def exportar_consolidado():
    """Exporta planilha de consolidação das vendas."""
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                vendas.c.id.label("ID Venda"),
                vendas.c.data_venda.label("Data venda"),
                produtos.c.nome.label("Produto"),
                produtos.c.sku.label("SKU"),
                vendas.c.quantidade.label("Quantidade"),
                vendas.c.preco_venda_unitario.label("Preço unitário"),
                vendas.c.receita_total.label("Receita total"),
                vendas.c.custo_total.label("Custo total"),
                vendas.c.margem_contribuicao.label("Margem contribuição"),
                vendas.c.origem.label("Origem"),
                vendas.c.numero_venda_ml.label("Nº venda ML"),
                vendas.c.lote_importacao.label("Lote importação"),
            ).select_from(vendas.join(produtos))
        ).mappings().all()

    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Consolidado")
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"consolidado_vendas_{datetime.now().date()}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/exportar_template")
def exportar_template():
    """Exporta o modelo de planilha para preenchimento manual (SKU, Título, Quantidade, Receita, Comissao, PrecoMedio)."""
    cols = ["SKU", "Título", "Quantidade", "Receita", "Comissao", "PrecoMedio"]
    df = pd.DataFrame(columns=cols)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Template")
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="template_consolidacao_vendas.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ---------------- ESTOQUE / AJUSTES ----------------
@app.route("/estoque")
def estoque_view():
    """Visão de estoque com médias reais dos últimos 30 dias
    + receita potencial (bruta - comissão ML)
    + lucro estimado (após custo, comissão, imposto e despesas).
    """

    JANELA_DIAS = 30     # últimos 30 dias sempre
    DIAS_MINIMOS = 15    # estoque mínimo desejado em dias

    hoje = datetime.now()
    limite_30dias = hoje - timedelta(days=JANELA_DIAS)

    with engine.connect() as conn:
        # Produtos
        produtos_rows = conn.execute(
            select(
                produtos.c.id,
                produtos.c.nome,
                produtos.c.sku,
                produtos.c.estoque_atual,
                produtos.c.custo_unitario,
            ).order_by(produtos.c.nome)
        ).mappings().all()

        # Vendas (para média dos últimos 30 dias)
        vendas_rows = conn.execute(
            select(
                vendas.c.produto_id,
                vendas.c.data_venda,
                vendas.c.quantidade,
            )
        ).mappings().all()

        # Configurações de imposto e despesas
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first() or {}

        imposto_percent = float(cfg.get("imposto_percent") or 0)
        despesas_percent = float(cfg.get("despesas_percent") or 0)

        # Agregado histórico de vendas por produto
        vendas_historico = conn.execute(
            select(
                vendas.c.produto_id,
                func.coalesce(func.sum(vendas.c.quantidade), 0).label("qtd"),
                func.coalesce(func.sum(vendas.c.receita_total), 0).label("receita"),
                func.coalesce(func.sum(vendas.c.custo_total), 0).label("custo"),
                func.coalesce(func.sum(vendas.c.margem_contribuicao), 0).label("margem_atual"),
            )
            .group_by(vendas.c.produto_id)
        ).mappings().all()

    # Indexa histórico por produto_id
    hist_por_produto = {h["produto_id"]: h for h in vendas_historico}

    # Soma das vendas por produto dentro da janela (últimos 30 dias)
    vendas_por_produto = {}

    for v in vendas_rows:
        pid = v["produto_id"]
        qtd = int(v["quantidade"] or 0)
        data_raw = v["data_venda"]

        if not data_raw:
            continue

        dt = parse_data_venda(data_raw)
        if not dt:
            try:
                dt = datetime.fromisoformat(str(data_raw))
            except Exception:
                continue

        if dt < limite_30dias or dt > hoje:
            continue

        vendas_por_produto[pid] = vendas_por_produto.get(pid, 0) + qtd

    produtos_enriquecidos = []

    total_unidades_estoque = 0.0
    total_custo_estoque = 0.0

    receita_potencial_total = 0.0     # receita bruta - comissão ML
    lucro_estimado_total = 0.0        # lucro líquido estimado

    for p in produtos_rows:
        pid = p["id"]
        estoque_atual = float(p["estoque_atual"] or 0)
        qtd_30dias = float(vendas_por_produto.get(pid, 0))
        custo_unitario = float(p["custo_unitario"] or 0)
        custo_estoque = estoque_atual * custo_unitario

        media_diaria = qtd_30dias / 30.0
        media_mensal = media_diaria * 30.0

        dias_cobertura = estoque_atual / media_diaria if media_diaria > 0 else None
        precisa_repor = dias_cobertura is not None and dias_cobertura < DIAS_MINIMOS

        h = hist_por_produto.get(pid)
        lucro_potencial = 0.0
        retorno_percent = 0.0
        receita_potencial_prod = 0.0

        if h:
            qtd_vendida = float(h["qtd"] or 0)
            receita_total = float(h["receita"] or 0)
            custo_total = float(h["custo"] or 0)
            margem_atual = float(h["margem_atual"] or 0)

            # Comissão ML estimada
            comissao_ml_total = max(0.0, (receita_total - custo_total) - margem_atual)
            imposto_val_total = receita_total * (imposto_percent / 100.0)
            despesas_val_total = receita_total * (despesas_percent / 100.0)

            lucro_liquido_total_hist = (
                receita_total
                - custo_total
                - comissao_ml_total
                - imposto_val_total
                - despesas_val_total
            )

            if qtd_vendida > 0:
                receita_unit = receita_total / qtd_vendida
                custo_unit_hist = custo_total / qtd_vendida
                comissao_unit = comissao_ml_total / qtd_vendida
                imposto_unit = imposto_val_total / qtd_vendida
                despesas_unit = despesas_val_total / qtd_vendida

                # receita potencial = receita bruta - comissão ML
                receita_potencial_prod = (receita_unit - comissao_unit) * estoque_atual

                lucro_liquido_unitario = (
                    receita_unit
                    - custo_unit_hist
                    - comissao_unit
                    - imposto_unit
                    - despesas_unit
                )
                lucro_potencial = lucro_liquido_unitario * estoque_atual

            if custo_estoque > 0:
                retorno_percent = (lucro_potencial / custo_estoque) * 100.0

        total_unidades_estoque += estoque_atual
        total_custo_estoque += custo_estoque
        receita_potencial_total += receita_potencial_prod
        lucro_estimado_total += lucro_potencial

        produtos_enriquecidos.append({
            "id": pid,
            "nome": p["nome"],
            "sku": p["sku"],
            "estoque_atual": estoque_atual,
            "custo_unitario": custo_unitario,
            "custo_estoque": custo_estoque,
            "media_diaria": media_diaria,
            "media_mensal": media_mensal,
            "dias_cobertura": dias_cobertura,
            "precisa_repor": precisa_repor,
            "lucro_potencial": lucro_potencial,
            "retorno_percent": retorno_percent,
        })

    if total_custo_estoque > 0:
        percentual_lucro_total = (lucro_estimado_total / total_custo_estoque) * 100.0
    else:
        percentual_lucro_total = 0.0

    return render_template(
        "estoque.html",
        produtos=produtos_enriquecidos,
        janela_dias=JANELA_DIAS,
        dias_minimos=DIAS_MINIMOS,
        total_unidades_estoque=total_unidades_estoque,
        total_custo_estoque=total_custo_estoque,
        receita_potencial_total=receita_potencial_total,
        lucro_estimado_total=lucro_estimado_total,
        percentual_lucro_total=percentual_lucro_total,
        imposto_percent=imposto_percent,
        despesas_percent=despesas_percent,
    )

# GET – formulário de ajuste
@app.route("/estoque/ajuste", methods=["GET"])
def ajuste_estoque_form():
    with engine.connect() as conn:
        produtos_rows = conn.execute(
            select(
                produtos.c.id,
                produtos.c.nome,
                produtos.c.sku
            ).order_by(produtos.c.nome)
        ).mappings().all()

    if not produtos_rows:
        flash("Cadastre ao menos 1 produto antes de ajustar estoque.", "warning")
        return redirect(url_for("estoque_view"))

    return render_template("ajuste_estoque.html", produtos=produtos_rows)

# POST – grava ajuste com custo médio ponderado
@app.route("/estoque/ajuste", methods=["POST"])
def ajuste_estoque():
    produto_id = int(request.form["produto_id"])
    tipo = request.form["tipo"]  # entrada ou saida
    quantidade = int(request.form.get("quantidade", 0) or 0)
    custo_unitario = request.form.get("custo_unitario")
    observacao = request.form.get("observacao") or ""

    custo_unitario_val = (
        float(custo_unitario) if custo_unitario not in (None, "",) else None
    )

    fator = 1 if tipo == "entrada" else -1

    with engine.begin() as conn:
        prod = conn.execute(
            select(
                produtos.c.estoque_atual,
                produtos.c.custo_unitario
            ).where(produtos.c.id == produto_id)
        ).mappings().first()

        if not prod:
            flash("Produto não encontrado para ajuste de estoque.", "danger")
            return redirect(url_for("estoque_view"))

        estoque_atual = float(prod["estoque_atual"] or 0)
        custo_atual = float(prod["custo_unitario"] or 0)

        novo_custo_medio = custo_atual

        if tipo == "entrada" and quantidade > 0 and custo_unitario_val is not None:
            if estoque_atual <= 0:
                novo_custo_medio = custo_unitario_val
            else:
                novo_custo_medio = (
                    (estoque_atual * custo_atual) + (quantidade * custo_unitario_val)
                ) / (estoque_atual + quantidade)

        novo_estoque = estoque_atual + fator * quantidade

        conn.execute(
            update(produtos)
            .where(produtos.c.id == produto_id)
            .values(
                estoque_atual=novo_estoque,
                custo_unitario=novo_custo_medio,
            )
        )

        if tipo == "saida":
            custo_ajuste_registro = custo_atual
        else:
            custo_ajuste_registro = custo_unitario_val

        conn.execute(
            insert(ajustes_estoque).values(
                produto_id=produto_id,
                data_ajuste=datetime.now().isoformat(),
                tipo=tipo,
                quantidade=quantidade,
                custo_unitario=custo_ajuste_registro,
                observacao=observacao,
            )
        )

    flash("Ajuste de estoque registrado com custo médio atualizado!", "success")
    return redirect(url_for("estoque_view"))

@app.route("/ajuste_estoque")
def ajuste_estoque_view():
    # mantém compatibilidade com URLs antigas, redirecionando para o novo formulário
    return redirect(url_for("ajuste_estoque_form"))
@app.route("/ajuste_estoque")
def ajuste_estoque_view():
    return render_template("ajuste_estoque.html")

# ---------------- CONFIGURAÇÕES ----------------
@app.route("/configuracoes", methods=["GET", "POST"])
def configuracoes_view():
    if request.method == "POST":
        imposto_percent = float(request.form.get("imposto_percent", 0) or 0)
        despesas_percent = float(request.form.get("despesas_percent", 0) or 0)
        with engine.begin() as conn:
            conn.execute(
                update(configuracoes)
                .where(configuracoes.c.id == 1)
                .values(imposto_percent=imposto_percent, despesas_percent=despesas_percent)
            )
        flash("Configurações salvas!", "success")
        return redirect(url_for("configuracoes_view"))

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

    return render_template("configuracoes.html", cfg=cfg)


# ---------------- RELATÓRIO LUCRO ----------------
@app.route("/relatorio_lucro")
def relatorio_lucro():
    """Relatório de lucro detalhado por produto, com filtro de período.

    Por padrão: mês vigente (do dia 1 até hoje).
    Margem líquida = Receita - Comissão ML - Custo - Despesas - Imposto
    """

    # --- período: vem da URL ou cai para mês vigente ---
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""

    if not data_inicio and not data_fim:
        hoje = date.today()
        inicio_mes = hoje.replace(day=1)
        data_inicio = inicio_mes.isoformat()
        data_fim = hoje.isoformat()

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes)
            .where(configuracoes.c.id == 1)
        ).mappings().first() or {}

        imposto_percent = float(cfg.get("imposto_percent") or 0)
        despesas_percent = float(cfg.get("despesas_percent") or 0)

        # monta query com filtro de datas
        query = (
            select(
                produtos.c.nome.label("produto"),
                func.sum(vendas.c.quantidade).label("qtd"),
                func.sum(vendas.c.receita_total).label("receita"),
                func.sum(vendas.c.custo_total).label("custo"),
                func.sum(vendas.c.margem_contribuicao).label("margem_atual"),
            )
            .select_from(vendas.join(produtos))
        )

        if data_inicio:
            query = query.where(vendas.c.data_venda >= data_inicio)
        if data_fim:
            query = query.where(vendas.c.data_venda <= data_fim + "T23:59:59")

        query = query.group_by(produtos.c.id)
        rows = conn.execute(query).mappings().all()

    linhas = []
    totais = {
        "qtd": 0.0,
        "receita": 0.0,
        "custo": 0.0,
        "comissao": 0.0,
        "imposto": 0.0,
        "despesas": 0.0,
        "margem_liquida": 0.0,
    }

    for r in rows:
        receita = float(r["receita"] or 0)
        custo = float(r["custo"] or 0)
        margem_atual = float(r["margem_atual"] or 0)

        # Comissão estimada do ML
        comissao_ml = max(0.0, (receita - custo) - margem_atual)

        imposto_val = receita * (imposto_percent / 100.0)
        despesas_val = receita * (despesas_percent / 100.0)

        margem_liquida = receita - custo - comissao_ml - imposto_val - despesas_val

        linha = {
            "produto": r["produto"],
            "qtd": float(r["qtd"] or 0),
            "receita": receita,
            "custo": custo,
            "comissao": comissao_ml,
            "imposto": imposto_val,
            "despesas": despesas_val,
            "margem_liquida": margem_liquida,
        }
        linhas.append(linha)

        totais["qtd"] += linha["qtd"]
        totais["receita"] += receita
        totais["custo"] += custo
        totais["comissao"] += comissao_ml
        totais["imposto"] += imposto_val
        totais["despesas"] += despesas_val
        totais["margem_liquida"] += margem_liquida

    # Ordena do maior lucro líquido para o menor
    linhas.sort(key=lambda x: x["margem_liquida"], reverse=True)

    return render_template(
        "relatorio_lucro.html",
        linhas=linhas,
        totais=totais,
        imposto_percent=imposto_percent,
        despesas_percent=despesas_percent,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )
@app.route("/relatorio_lucro/exportar")
def relatorio_lucro_exportar():
    # mesmo critério de período do relatorio_lucro
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""

    if not data_inicio and not data_fim:
        hoje = date.today()
        inicio_mes = hoje.replace(day=1)
        data_inicio = inicio_mes.isoformat()
        data_fim = hoje.isoformat()

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes)
            .where(configuracoes.c.id == 1)
        ).mappings().first() or {}

        imposto_percent = float(cfg.get("imposto_percent") or 0)
        despesas_percent = float(cfg.get("despesas_percent") or 0)

        query = (
            select(
                produtos.c.nome.label("produto"),
                func.sum(vendas.c.quantidade).label("qtd"),
                func.sum(vendas.c.receita_total).label("receita"),
                func.sum(vendas.c.custo_total).label("custo"),
                func.sum(vendas.c.margem_contribuicao).label("margem_atual"),
            )
            .select_from(vendas.join(produtos))
        )

        if data_inicio:
            query = query.where(vendas.c.data_venda >= data_inicio)
        if data_fim:
            query = query.where(vendas.c.data_venda <= data_fim + "T23:59:59")

        query = query.group_by(produtos.c.id)
        rows = conn.execute(query).mappings().all()

    linhas_export = []

    for r in rows:
        receita = float(r["receita"] or 0)
        custo = float(r["custo"] or 0)
        margem_atual = float(r["margem_atual"] or 0)
        qtd = float(r["qtd"] or 0)

        comissao_ml = max(0.0, (receita - custo) - margem_atual)
        imposto_val = receita * (imposto_percent / 100.0)
        despesas_val = receita * (despesas_percent / 100.0)
        margem_liquida = receita - custo - comissao_ml - imposto_val - despesas_val

        linhas_export.append({
            "Produto": r["produto"],
            "Quantidade": qtd,
            "Receita (R$)": receita,
            "Custo (R$)": custo,
            "Comissão ML (R$)": comissao_ml,
            "Imposto (R$)": imposto_val,
            "Despesas (R$)": despesas_val,
            "Lucro líquido (R$)": margem_liquida,
        })

    df = pd.DataFrame(linhas_export)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="RelatorioLucro")
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"relatorio_lucro_{datetime.now().date()}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
# --------------------------------------------------------------------
# Inicialização
# --------------------------------------------------------------------
init_db()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
