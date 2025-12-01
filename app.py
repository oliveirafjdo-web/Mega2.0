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
    with engine.connect() as conn:
        # Totais básicos
        total_produtos = conn.execute(
            select(func.count()).select_from(produtos)
        ).scalar_one()

        estoque_total = conn.execute(
            select(func.coalesce(func.sum(produtos.c.estoque_atual), 0))
        ).scalar_one()

        # Totais de vendas: MESMA BASE DO RELATÓRIO DE LUCRO
        receita_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.receita_total), 0))
        ).scalar_one()

        custo_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.custo_total), 0))
        ).scalar_one()

        margem_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.margem_contribuicao), 0))
        ).scalar_one()

        # Configurações (imposto / despesas)
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

        imposto_percent = float(cfg["imposto_percent"]) if cfg else 0.0
        despesas_percent = float(cfg["despesas_percent"]) if cfg else 0.0

        # Comissão estimada (mesma lógica do relatório de lucro)
        comissao_total = (receita_total - custo_total) - margem_total
        if comissao_total < 0:
            comissao_total = 0.0

        # Imposto e despesas totais
        imposto_total = receita_total * (imposto_percent / 100.0)
        despesas_total = receita_total * (despesas_percent / 100.0)

        # Receita líquida (bruta - comissão - imposto - despesas)
        receita_liquida_total = receita_total - comissao_total - imposto_total - despesas_total

        # Lucro líquido
        lucro_liquido_total = (
            receita_total
            - custo_total
            - comissao_total
            - imposto_total
            - despesas_total
        )

        # Margem líquida em %
        margem_liquida_percent = (
            (lucro_liquido_total / receita_total) * 100.0
            if receita_total > 0
            else 0.0
        )

        # Margem média de contribuição
        margem_ = (
            (margem_total / receita_total) * 100.0
            if receita_total > 0
            else 0.0
        )

        # Ticket médio
        ticket_medio = conn.execute(
            select(func.coalesce(func.avg(vendas.c.preco_venda_unitario), 0))
        ).scalar_one()

        produto_mais_vendido = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.quantidade).label("qtd"))
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.quantidade).desc())
            .limit(1)
        ).first()

        produto_maior_lucro = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("lucro"))
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).desc())
            .limit(1)
        ).first()

        produto_pior_margem = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("margem"))
            .select_from(vendas.join(produtos))
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
        cfg=cfg
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
@app.route("/vendas")
def lista_vendas():
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""

    with engine.connect() as conn:
        # Query base das vendas
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

        if data_inicio:
            query_vendas = query_vendas.where(vendas.c.data_venda >= data_inicio)

        if data_fim:
            query_vendas = query_vendas.where(vendas.c.data_venda <= data_fim + "T23:59:59")

        query_vendas = query_vendas.order_by(vendas.c.data_venda.asc())
        vendas_rows = conn.execute(query_vendas).mappings().all()

        # Lotes
        query_lotes = select(
            vendas.c.lote_importacao.label("lote_importacao"),
            func.count().label("qtd_vendas"),
            func.coalesce(func.sum(vendas.c.receita_total), 0).label("receita_lote"),
        ).where(vendas.c.lote_importacao.isnot(None))

        if data_inicio:
            query_lotes = query_lotes.where(vendas.c.data_venda >= data_inicio)

        if data_fim:
            query_lotes = query_lotes.where(vendas.c.data_venda <= data_fim + "T23:59:59")

        query_lotes = query_lotes.group_by(vendas.c.lote_importacao)
        lotes = conn.execute(query_lotes).mappings().all()

        produtos_rows = conn.execute(
            select(produtos.c.id, produtos.c.nome).order_by(produtos.c.nome)
        ).mappings().all()

    # =======================
    # CALCULOS PARA GRÁFICOS
    # =======================
    faturamento_dia = {}
    quantidade_dia = {}
    lucro_dia = {}

    for v in vendas_rows:
        if not v["data_venda"]:
            continue

        try:
            dt = datetime.fromisoformat(v["data_venda"]).date()
        except:
            continue

        receita = float(v["receita_total"] or 0)
        custo = float(v["custo_total"] or 0)
        margem = float(v["margem_contribuicao"] or 0)
        qtd = float(v["quantidade"] or 0)

        lucro = receita - custo - max(0.0, (receita - custo) - margem)

        # faturamento diário
        faturamento_dia[dt] = faturamento_dia.get(dt, 0) + receita

        # quantidade por dia
        quantidade_dia[dt] = quantidade_dia.get(dt, 0) + qtd

        # lucro diário
        lucro_dia[dt] = lucro_dia.get(dt, 0) + lucro

    grafico_labels = [d.isoformat() for d in sorted(faturamento_dia.keys())]
    grafico_faturamento = [faturamento_dia[d] for d in sorted(faturamento_dia.keys())]
    grafico_quantidade = [quantidade_dia.get(d, 0) for d in sorted(faturamento_dia.keys())]
    grafico_lucro = [lucro_dia.get(d, 0) for d in sorted(faturamento_dia.keys())]

    # =========================
    # MÊS ATUAL vs MÊS ANTERIOR
    # =========================

    hoje = date.today()
    inicio_mes_atual = hoje.replace(day=1)
    if inicio_mes_atual.month == 1:
        inicio_mes_anterior = inicio_mes_atual.replace(year=inicio_mes_atual.year - 1, month=12)
    else:
        inicio_mes_anterior = inicio_mes_atual.replace(month=inicio_mes_atual.month - 1)

    faturamento_mes_atual = {}
    faturamento_mes_anterior = {}

    for v in vendas_rows:
        if not v["data_venda"]:
            continue

        dt = datetime.fromisoformat(v["data_venda"]).date()

        if dt >= inicio_mes_atual:
            faturamento_mes_atual[dt] = faturamento_mes_atual.get(dt, 0) + float(v["receita_total"] or 0)

        if dt >= inicio_mes_anterior and dt < inicio_mes_atual:
            faturamento_mes_anterior[dt] = faturamento_mes_anterior.get(dt, 0) + float(v["receita_total"] or 0)

    grafico_cmp_labels = [d.isoformat() for d in sorted(faturamento_mes_atual.keys())]
    grafico_cmp_atual = [faturamento_mes_atual[d] for d in sorted(faturamento_mes_atual.keys())]
    grafico_cmp_anterior = [faturamento_mes_anterior.get(
        (inicio_mes_atual.replace(day=1) + (d - inicio_mes_atual.replace(day=1))), 0
    ) for d in sorted(faturamento_mes_atual.keys())]

    # TOTAIS
    totais = {
        "qtd": sum(q["quantidade"] for q in vendas_rows),
        "receita": sum(q["receita_total"] for q in vendas_rows),
        "custo": sum(q["custo_total"] for q in vendas_rows),
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
    )

@app.route("/vendas/manual", methods=["POST"])
def criar_venda_manual():
    produto_id = int(request.form["produto_id"])
    quantidade = int(request.form.get("quantidade", 0) or 0)
    preco_unit = float(request.form.get("preco_venda_unitario", 0) or 0)
    data_venda_str = request.form.get("data_venda") or datetime.now().isoformat()

    with engine.begin() as conn:
        prod = conn.execute(
            select(produtos.c.custo_unitario).where(produtos.c.id == produto_id)
        ).mappings().first()
        custo_unitario = float(prod["custo_unitario"] or 0.0) if prod else 0.0

        receita_total = quantidade * preco_unit
        custo_total = quantidade * custo_unitario
        margem_contribuicao = receita_total - custo_total

        conn.execute(
            insert(vendas).values(
                produto_id=produto_id,
                data_venda=data_venda_str,
                quantidade=quantidade,
                preco_venda_unitario=preco_unit,
                receita_total=receita_total,
                custo_total=custo_total,
                margem_contribuicao=margem_contribuicao,
                origem="Manual",
                numero_venda_ml=None,
                lote_importacao=None,
            )
        )

        conn.execute(
            update(produtos)
            .where(produtos.c.id == produto_id)
            .values(estoque_atual=produtos.c.estoque_atual - quantidade)
        )

    flash("Venda manual registrada com sucesso!", "success")
    return redirect(url_for("lista_vendas"))


@app.route("/vendas/<int:venda_id>/editar", methods=["GET", "POST"])
def editar_venda(venda_id):
    if request.method == "POST":
        quantidade = int(request.form["quantidade"])
        preco_venda_unitario = float(request.form["preco_venda_unitario"])
        custo_total = float(request.form["custo_total"])

        receita_total = quantidade * preco_venda_unitario
        margem_contribuicao = receita_total - custo_total

        with engine.begin() as conn:
            conn.execute(
                update(vendas)
                .where(vendas.c.id == venda_id)
                .values(
                    quantidade=quantidade,
                    preco_venda_unitario=preco_venda_unitario,
                    receita_total=receita_total,
                    margem_contribuicao=margem_contribuicao,
                )
            )
        flash("Venda atualizada com sucesso!", "success")
        return redirect(url_for("lista_vendas"))

    with engine.connect() as conn:
        venda_row = conn.execute(
            select(
                vendas.c.id,
                vendas.c.data_venda,
                vendas.c.quantidade,
                vendas.c.preco_venda_unitario,
                vendas.c.custo_total,
                produtos.c.nome,
            )
            .select_from(vendas.join(produtos))
            .where(vendas.c.id == venda_id)
        ).mappings().first()

    if not venda_row:
        flash("Venda não encontrada.", "danger")
        return redirect(url_for("lista_vendas"))

    return render_template("editar_venda.html", venda=venda_row)


@app.route("/vendas/<int:venda_id>/excluir", methods=["POST"])
def excluir_venda(venda_id):
    with engine.begin() as conn:
        conn.execute(delete(vendas).where(vendas.c.id == venda_id))
    flash("Venda excluída com sucesso!", "success")
    return redirect(url_for("lista_vendas"))


@app.route("/vendas/lote/<lote_id>/excluir", methods=["POST"])
def excluir_lote_vendas(lote_id):
    with engine.begin() as conn:
        conn.execute(delete(vendas).where(vendas.c.lote_importacao == lote_id))
    flash("Lote de importação excluído com sucesso!", "success")
    return redirect(url_for("lista_vendas"))


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
    """Visão de estoque com médias reais dos últimos 30 dias."""

    JANELA_DIAS = 30     # FIXO: últimos 30 dias sempre
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

        # Vendas (todas, vamos filtrar via Python)
        vendas_rows = conn.execute(
            select(
                vendas.c.produto_id,
                vendas.c.data_venda,
                vendas.c.quantidade,
            )
        ).mappings().all()

    # Soma das vendas por produto dentro da janela (últimos 30 dias)
    vendas_por_produto = {}

    for v in vendas_rows:
        pid = v["produto_id"]
        qtd = int(v["quantidade"] or 0)
        data_raw = v["data_venda"]

        if not data_raw:
            continue

        # tenta converter para datetime
        dt = None
        try:
            dt = parse_data_venda(data_raw)
        except:
            try:
                dt = datetime.fromisoformat(str(data_raw))
            except:
                continue

        if dt is None:
            continue

        # só considera vendas dentro dos últimos 30 dias
        if dt < limite_30dias or dt > hoje:
            continue

        vendas_por_produto[pid] = vendas_por_produto.get(pid, 0) + qtd

    # Construção da tabela
    produtos_enriquecidos = []
    total_unidades_estoque = 0
    total_custo_estoque = 0

    for p in produtos_rows:
        pid = p["id"]
        estoque_atual = float(p["estoque_atual"] or 0)
        qtd_30dias = float(vendas_por_produto.get(pid, 0))
        custo_unitario = float(p["custo_unitario"] or 0)
        custo_estoque = estoque_atual * custo_unitario

        # Média diária usando sempre 30 dias
        media_diaria = qtd_30dias / 30.0
        media_mensal = media_diaria * 30.0

        # Cobertura
        dias_cobertura = estoque_atual / media_diaria if media_diaria > 0 else None

        precisa_repor = dias_cobertura is not None and dias_cobertura < DIAS_MINIMOS

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
        })

        total_unidades_estoque += estoque_atual
        total_custo_estoque += custo_estoque

    return render_template(
        "estoque.html",
        produtos=produtos_enriquecidos,
        janela_dias=30,
        dias_minimos=DIAS_MINIMOS,
        total_unidades_estoque=total_unidades_estoque,
        total_custo_estoque=total_custo_estoque,
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

        # só recalcula custo em ENTRADA com custo informado
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
    """Relatório de lucro detalhado por produto.

    Margem líquida = Receita - Comissão ML - Custo - Despesas - Imposto
    """

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes)
            .where(configuracoes.c.id == 1)
        ).mappings().first() or {}

        imposto_percent = float(cfg.get("imposto_percent") or 0)
        despesas_percent = float(cfg.get("despesas_percent") or 0)

        rows = conn.execute(
            select(
                produtos.c.nome.label("produto"),
                func.sum(vendas.c.quantidade).label("qtd"),
                func.sum(vendas.c.receita_total).label("receita"),
                func.sum(vendas.c.custo_total).label("custo"),
                func.sum(vendas.c.margem_contribuicao).label("margem_atual"),
            )
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
        ).mappings().all()

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

    linhas.sort(key=lambda x: x["margem_liquida"], reverse=True)

    return render_template(
        "relatorio_lucro.html",
        linhas=linhas,
        totais=totais,
        imposto_percent=imposto_percent,
        despesas_percent=despesas_percent,
    )
@app.route("/relatorio_lucro/exportar")
def relatorio_lucro_exportar():
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                produtos.c.nome.label("Produto"),
                func.sum(vendas.c.quantidade).label("Quantidade"),
                func.sum(vendas.c.receita_total).label("Receita"),
                func.sum(vendas.c.custo_total).label("Custo"),
                func.sum(vendas.c.margem_contribuicao).label("Margem"),
            )
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
        ).mappings().all()

    import pandas as pd
    from io import BytesIO

    df = pd.DataFrame(rows)

    # Criar arquivo Excel na memória
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="RelatorioLucro")

    output.seek(0)

    from flask import send_file
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
