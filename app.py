import os
from datetime import datetime, date, timedelta
from io import BytesIO

from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, send_file, session
)
from werkzeug.utils import secure_filename
from functools import wraps

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float,
    ForeignKey, func, select, insert, update, delete
)
from sqlalchemy.engine import Engine
import pandas as pd


# ----------------------------------------------------------------------------
# CONFIGURAÇÃO BÁSICA DO APP
# ----------------------------------------------------------------------------

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "metrifypremium-secret")

# Usuário admin (defina no Render em ENV VARS)
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")

# Pasta de upload
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER


# ----------------------------------------------------------------------------
# CONFIGURAÇÃO DO BANCO (POSTGRES NO RENDER / SQLITE LOCAL)
# ----------------------------------------------------------------------------

raw_db_url = os.environ.get("DATABASE_URL")

if raw_db_url:
    # Render às vezes entrega "postgres://", o SQLAlchemy quer "postgresql+psycopg2://"
    if raw_db_url.startswith("postgres://"):
        raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    DATABASE_URL = raw_db_url
else:
    # fallback local
    DATABASE_URL = "sqlite:////tmp/metrifiy.db"

engine: Engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()


# ----------------------------------------------------------------------------
# DEFINIÇÃO DAS TABELAS
# ----------------------------------------------------------------------------

produtos = Table(
    "produtos", metadata,
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
    "vendas", metadata,
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
    "ajustes_estoque", metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, ForeignKey("produtos.id"), nullable=False),
    Column("data_ajuste", String(50)),
    Column("tipo", String(20)),  # entrada ou saida
    Column("quantidade", Integer),
    Column("custo_unitario", Float),
    Column("observacao", String(255)),
)

configuracoes = Table(
    "configuracoes", metadata,
    Column("id", Integer, primary_key=True),
    Column("imposto_percent", Float, nullable=False, server_default="0"),
    Column("despesas_percent", Float, nullable=False, server_default="0"),
)


# ----------------------------------------------------------------------------
# INICIALIZAÇÃO DO BANCO
# ----------------------------------------------------------------------------

def init_db():
    """Cria as tabelas se não existirem e garante 1 linha em configuracoes."""
    metadata.create_all(engine)
    with engine.begin() as conn:
        row = conn.execute(
            select(configuracoes.c.id).limit(1)
        ).first()
        if not row:
            conn.execute(
                insert(configuracoes).values(
                    id=1,
                    imposto_percent=0.0,
                    despesas_percent=0.0
                )
            )


# ----------------------------------------------------------------------------
# DECORATOR DE LOGIN
# ----------------------------------------------------------------------------

def login_required(view_func):
    """Decorator para proteger rotas. Redireciona para /login se não estiver logado."""
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        if not session.get("logged_in"):
            next_url = request.path
            return redirect(url_for("login", next=next_url))
        return view_func(*args, **kwargs)
    return wrapped_view


# ----------------------------------------------------------------------------
# ROTAS DE LOGIN / LOGOUT
# ----------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if username == ADMIN_USER and password == ADMIN_PASSWORD:
            session["logged_in"] = True
            session["username"] = username
            flash("Login realizado com sucesso!", "success")

            next_url = request.args.get("next") or url_for("dashboard")
            return redirect(next_url)

        flash("Usuário ou senha inválidos.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Você saiu do sistema.", "info")
    return redirect(url_for("login"))
# ----------------------------------------------------------------------------
# CONFIGURAÇÃO DO BANCO (POSTGRES OU SQLITE)
# ----------------------------------------------------------------------------

raw_db_url = os.environ.get("DATABASE_URL")

if raw_db_url:
    # Render usa postgres:// (incompatível com SQLAlchemy)
    if raw_db_url.startswith("postgres://"):
        raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    DATABASE_URL = raw_db_url
else:
    DATABASE_URL = "sqlite:////tmp/metrifiy.db"

engine: Engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()


# ----------------------------------------------------------------------------
# TABELAS DO BANCO
# ----------------------------------------------------------------------------

produtos = Table(
    "produtos", metadata,
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
    "vendas", metadata,
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
    "ajustes_estoque", metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, ForeignKey("produtos.id"), nullable=False),
    Column("data_ajuste", String(50)),
    Column("tipo", String(20)),   # entrada ou saida
    Column("quantidade", Integer),
    Column("custo_unitario", Float),
    Column("observacao", String(255)),
)

configuracoes = Table(
    "configuracoes", metadata,
    Column("id", Integer, primary_key=True),
    Column("imposto_percent", Float, nullable=False, server_default="0"),
    Column("despesas_percent", Float, nullable=False, server_default="0"),
)


# ----------------------------------------------------------------------------
# CRIA O BANCO SE NÃO EXISTIR
# ----------------------------------------------------------------------------

def init_db():
    metadata.create_all(engine)
    with engine.begin() as conn:
        if not conn.execute(select(configuracoes.c.id)).fetchone():
            conn.execute(insert(configuracoes).values(
                id=1, imposto_percent=0.0, despesas_percent=0.0
            ))


init_db()
# ----------------------------------------------------------------------------
# UTILITÁRIOS DE DATA
# ----------------------------------------------------------------------------

MESES_PT = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}

def parse_data_venda(texto):
    """Interpreta datas vindas da planilha do Mercado Livre."""
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
        return None


# ----------------------------------------------------------------------------
# IMPORTAÇÃO DE VENDAS DO MERCADO LIVRE
# ----------------------------------------------------------------------------

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
            except:
                unidades = 0

            total_brl = row.get("Total (BRL)")
            try:
                receita_total = float(total_brl) if total_brl == total_brl else 0.0
            except:
                receita_total = 0.0

            preco_medio_venda = receita_total / unidades if unidades > 0 else 0.0
            custo_total = custo_unitario * unidades

            tarifa = row.get("Tarifa de venda e impostos (BRL)")
            try:
                comissao_ml = float(tarifa) if tarifa == tarifa else 0.0
            except:
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
    # ----------------------------------------------------------------------------
# ROTAS PRINCIPAIS
# ----------------------------------------------------------------------------

@app.route("/")
@login_required
def dashboard():
    """Dashboard com filtro opcional de período."""

    data_inicio = request.args.get("data_inicio")
    data_fim = request.args.get("data_fim")

    # Se não houver filtro → usa o mês atual
    hoje = date.today()
    primeiro_dia = hoje.replace(day=1)

    if not data_inicio:
        data_inicio = primeiro_dia.isoformat()
    if not data_fim:
        data_fim = hoje.isoformat()

    with engine.connect() as conn:
        # Totais do período filtrado
        query = select(
            func.sum(vendas.c.receita_total).label("receita"),
            func.sum(vendas.c.custo_total).label("custo"),
            func.sum(vendas.c.margem_contribuicao).label("margem")
        )

        query = query.where(
            vendas.c.data_venda >= data_inicio,
        ).where(
            vendas.c.data_venda <= data_fim + "T23:59:59"
        )

        tot = conn.execute(query).mappings().first()

        receita_total = float(tot["receita"] or 0)
        custo_total = float(tot["custo"] or 0)
        margem_total = float(tot["margem"] or 0)

        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

        imposto_percent = float(cfg["imposto_percent"] or 0)
        despesas_percent = float(cfg["despesas_percent"] or 0)

        comissao_total = max(0, (receita_total - custo_total) - margem_total)
        imposto_total = receita_total * (imposto_percent / 100)
        despesas_total = receita_total * (despesas_percent / 100)

        lucro_liquido_total = receita_total - custo_total - comissao_total - imposto_total - despesas_total

        margem_liquida_percent = (lucro_liquido_total / receita_total * 100) if receita_total > 0 else 0

        # Ticket médio
        ticket_medio = conn.execute(
            select(func.avg(vendas.c.preco_venda_unitario)).where(
                vendas.c.data_venda >= data_inicio,
                vendas.c.data_venda <= data_fim + "T23:59:59",
            )
        ).scalar() or 0

        # Produtos em estoque
        estoque_total = conn.execute(
            select(func.sum(produtos.c.estoque_atual))
        ).scalar() or 0

        total_produtos = conn.execute(
            select(func.count(produtos.c.id))
        ).scalar() or 0

    return render_template(
        "dashboard.html",
        receita_total=receita_total,
        custo_total=custo_total,
        margem_total=margem_total,
        lucro_liquido_total=lucro_liquido_total,
        margem_liquida_percent=margem_liquida_percent,
        comissao_total=comissao_total,
        imposto_total=imposto_total,
        despesas_total=despesas_total,
        ticket_medio=ticket_medio,
        estoque_total=estoque_total,
        total_produtos=total_produtos,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )


# ----------------------------------------------------------------------------
# PRODUTOS
# ----------------------------------------------------------------------------

@app.route("/produtos")
@login_required
def lista_produtos():
    with engine.connect() as conn:
        produtos_rows = conn.execute(
            select(produtos).order_by(produtos.c.nome)
        ).mappings().all()
    return render_template("produtos.html", produtos=produtos_rows)


@app.route("/produtos/novo", methods=["GET", "POST"])
@login_required
def novo_produto():
    if request.method == "POST":
        nome = request.form["nome"]
        sku = request.form["sku"]
        custo_unitario = float(request.form.get("custo_unitario", 0))
        preco_venda_sugerido = float(request.form.get("preco_venda_sugerido", 0))
        estoque_inicial = int(request.form.get("estoque_inicial", 0))

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
@login_required
def editar_produto(produto_id):
    if request.method == "POST":
        nome = request.form["nome"]
        sku = request.form["sku"]
        custo_unitario = float(request.form.get("custo_unitario", 0))
        preco_venda_sugerido = float(request.form.get("preco_venda_sugerido", 0))
        estoque_atual = int(request.form.get("estoque_atual", 0))

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
@login_required
def excluir_produto(produto_id):
    with engine.begin() as conn:
        conn.execute(delete(produtos).where(produtos.c.id == produto_id))

    flash("Produto excluído!", "success")
    return redirect(url_for("lista_produtos"))
    # ----------------------------------------------------------------------------
# VENDAS
# ----------------------------------------------------------------------------

@app.route("/vendas")
@login_required
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

        # Filtros por data (data_venda em ISO: 2025-11-20T00:00:00)
        if data_inicio:
            query_vendas = query_vendas.where(vendas.c.data_venda >= data_inicio)
        if data_fim:
            query_vendas = query_vendas.where(vendas.c.data_venda <= data_fim + "T23:59:59")

        query_vendas = query_vendas.order_by(vendas.c.data_venda.desc(), vendas.c.id.desc())
        vendas_rows = conn.execute(query_vendas).mappings().all()

        # Lotes (respeitando os mesmos filtros)
        query_lotes = select(
            vendas.c.lote_importacao.label("lote_importacao"),
            func.count().label("qtd_vendas"),
            func.coalesce(func.sum(vendas.c.receita_total), 0).label("receita_lote"),
        ).where(vendas.c.lote_importacao.isnot(None))

        if data_inicio:
            query_lotes = query_lotes.where(vendas.c.data_venda >= data_inicio)
        if data_fim:
            query_lotes = query_lotes.where(vendas.c.data_venda <= data_fim + "T23:59:59")

        query_lotes = query_lotes.group_by(vendas.c.lote_importacao).order_by(vendas.c.lote_importacao.desc())
        lotes = conn.execute(query_lotes).mappings().all()

        produtos_rows = conn.execute(
            select(produtos.c.id, produtos.c.nome).order_by(produtos.c.nome)
        ).mappings().all()

    # --------- Cálculo dos totais da lista de vendas ----------
    totais = {
        "qtd": 0.0,
        "receita": 0.0,
        "custo": 0.0,
        "margem": 0.0,
        "comissao": 0.0,  # comissão estimada
    }

    for v in vendas_rows:
        qtd = float(v["quantidade"] or 0)
        receita = float(v["receita_total"] or 0)
        custo = float(v["custo_total"] or 0)
        margem = float(v["margem_contribuicao"] or 0)

        comissao_est = max(0.0, (receita - custo) - margem)

        totais["qtd"] += qtd
        totais["receita"] += receita
        totais["custo"] += custo
        totais["margem"] += margem
        totais["comissao"] += comissao_est

    return render_template(
        "vendas.html",
        vendas=vendas_rows,
        lotes=lotes,
        produtos=produtos_rows,
        data_inicio=data_inicio,
        data_fim=data_fim,
        totais=totais,
    )


@app.route("/vendas/manual", methods=["POST"])
@login_required
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
@login_required
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
@login_required
def excluir_venda(venda_id):
    with engine.begin() as conn:
        conn.execute(delete(vendas).where(vendas.c.id == venda_id))
    flash("Venda excluída com sucesso!", "success")
    return redirect(url_for("lista_vendas"))


@app.route("/vendas/lote/<lote_id>/excluir", methods=["POST"])
@login_required
def excluir_lote_vendas(lote_id):
    with engine.begin() as conn:
        conn.execute(delete(vendas).where(vendas.c.lote_importacao == lote_id))
    flash("Lote de importação excluído com sucesso!", "success")
    return redirect(url_for("lista_vendas"))


# ----------------------------------------------------------------------------
# IMPORT / EXPORT
# ----------------------------------------------------------------------------

@app.route("/importar_ml", methods=["GET", "POST"])
@login_required
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
@login_required
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
@login_required
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
    # ----------------------------------------------------------------------------
# ESTOQUE / AJUSTES
# ----------------------------------------------------------------------------

@app.route("/estoque")
@login_required
def estoque_view():
    """
    Visão de estoque com médias de venda e previsão de cobertura.

    - média_diaria: média de unidades vendidas por dia nos últimos 30 dias
    - média_mensal: média_diaria * 30
    - dias_cobertura: estoque_atual / média_diaria
    - precisa_repor: True se dias_cobertura < dias_minimos
    """

    JANELA_DIAS = 30      # quantos dias olhar pra trás nas vendas
    DIAS_MINIMOS = 15     # estoque mínimo desejado em dias

    hoje = date.today()
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

        # Vendas (só o necessário)
        vendas_rows = conn.execute(
            select(
                vendas.c.produto_id,
                vendas.c.data_venda,
                vendas.c.quantidade,
            )
        ).mappings().all()

    # Soma de vendas por produto dentro da janela (últimos 30 dias)
    vendas_por_produto = {}

    for v in vendas_rows:
        pid = v["produto_id"]
        qtd = int(v["quantidade"] or 0)
        data_raw = v["data_venda"]

        if not data_raw:
            continue

        # tenta interpretar a data
        dt = parse_data_venda(data_raw)
        if dt is None:
            try:
                dt = datetime.fromisoformat(str(data_raw))
            except Exception:
                continue

        dt_date = dt.date()
        # só considera vendas dentro da janela
        if dt_date < limite_30dias or dt_date > hoje:
            continue

        vendas_por_produto[pid] = vendas_por_produto.get(pid, 0) + qtd

    # Monta lista enriquecida de produtos + totais de estoque
    produtos_enriquecidos = []
    total_unidades_estoque = 0.0
    total_custo_estoque = 0.0

    for p in produtos_rows:
        pid = p["id"]
        estoque_atual = float(p["estoque_atual"] or 0)
        qtd_periodo = float(vendas_por_produto.get(pid, 0))
        custo_unitario = float(p["custo_unitario"] or 0)
        custo_estoque = estoque_atual * custo_unitario

        # média diária baseada na janela fixa de 30 dias
        media_diaria = qtd_periodo / JANELA_DIAS if JANELA_DIAS > 0 else 0.0
        media_mensal = media_diaria * 30.0

        if media_diaria > 0:
            dias_cobertura = estoque_atual / media_diaria
        else:
            dias_cobertura = None  # sem vendas recentes, não dá pra estimar

        precisa_repor = (
            dias_cobertura is not None and dias_cobertura < DIAS_MINIMOS
        )

        produtos_enriquecidos.append(
            {
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
            }
        )

        total_unidades_estoque += estoque_atual
        total_custo_estoque += custo_estoque

    return render_template(
        "estoque.html",
        produtos=produtos_enriquecidos,
        janela_dias=JANELA_DIAS,
        dias_minimos=DIAS_MINIMOS,
        total_unidades_estoque=total_unidades_estoque,
        total_custo_estoque=total_custo_estoque,
    )


@app.route("/estoque/ajuste", methods=["POST"])
@login_required
def ajuste_estoque():
    produto_id = int(request.form["produto_id"])
    tipo = request.form["tipo"]  # entrada ou saida
    quantidade = int(request.form.get("quantidade", 0) or 0)
    custo_unitario = request.form.get("custo_unitario")
    observacao = request.form.get("observacao") or ""

    custo_unitario_val = float(custo_unitario) if custo_unitario not in (None, "",) else None

    fator = 1 if tipo == "entrada" else -1

    with engine.begin() as conn:
        # Se informou novo custo, atualiza o produto
        if custo_unitario_val is not None:
            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(custo_unitario=custo_unitario_val)
            )

        # Ajusta estoque
        conn.execute(
            update(produtos)
            .where(produtos.c.id == produto_id)
            .values(estoque_atual=produtos.c.estoque_atual + fator * quantidade)
        )

        # Registra o ajuste
        conn.execute(
            insert(ajustes_estoque).values(
                produto_id=produto_id,
                data_ajuste=datetime.now().isoformat(),
                tipo=tipo,
                quantidade=quantidade,
                custo_unitario=custo_unitario_val,
                observacao=observacao,
            )
        )

    flash("Ajuste de estoque registrado!", "success")
    return redirect(url_for("estoque_view"))
    # ----------------------------------------------------------------------------
# CONFIGURAÇÕES (Imposto / Despesas)
# ----------------------------------------------------------------------------

@app.route("/configuracoes", methods=["GET", "POST"])
@login_required
def configuracoes_view():
    if request.method == "POST":
        imposto_percent = float(request.form.get("imposto_percent", 0) or 0)
        despesas_percent = float(request.form.get("despesas_percent", 0) or 0)

        with engine.begin() as conn:
            conn.execute(
                update(configuracoes)
                .where(configuracoes.c.id == 1)
                .values(
                    imposto_percent=imposto_percent,
                    despesas_percent=despesas_percent
                )
            )

        flash("Configurações salvas!", "success")
        return redirect(url_for("configuracoes_view"))

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

    return render_template("configuracoes.html", cfg=cfg)



# ----------------------------------------------------------------------------
# RELATÓRIO DE LUCRO — COM FILTRO DE PERÍODO
# ----------------------------------------------------------------------------

@app.route("/relatorio_lucro")
@login_required
def relatorio_lucro():
    """Relatório de lucro detalhado por produto, com filtro de período."""

    data_inicio = request.args.get("data_inicio")
    data_fim = request.args.get("data_fim")

    # Se não houver filtro → mês vigente
    hoje = date.today()
    primeiro_dia = hoje.replace(day=1)

    if not data_inicio:
        data_inicio = primeiro_dia.isoformat()
    if not data_fim:
        data_fim = hoje.isoformat()

    with engine.connect() as conn:

        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first() or {}

        imposto_percent = float(cfg.get("imposto_percent") or 0)
        despesas_percent = float(cfg.get("despesas_percent") or 0)

        # Consolida vendas por produto dentro do período
        rows = conn.execute(
            select(
                produtos.c.nome.label("produto"),
                func.sum(vendas.c.quantidade).label("qtd"),
                func.sum(vendas.c.receita_total).label("receita"),
                func.sum(vendas.c.custo_total).label("custo"),
                func.sum(vendas.c.margem_contribuicao).label("margem_atual"),
            )
            .select_from(vendas.join(produtos))
            .where(vendas.c.data_venda >= data_inicio)
            .where(vendas.c.data_venda <= data_fim + "T23:59:59")
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

        # Comissão ML estimada
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

        # Totais gerais
        totais["qtd"] += linha["qtd"]
        totais["receita"] += receita
        totais["custo"] += custo
        totais["comissao"] += comissao_ml
        totais["imposto"] += imposto_val
        totais["despesas"] += despesas_val
        totais["margem_liquida"] += margem_liquida

    # Ordenação do maior lucro para o menor
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



# ----------------------------------------------------------------------------
# EXPORTAR RELATÓRIO DE LUCRO — COM FILTRO
# ----------------------------------------------------------------------------

@app.route("/exportar_relatorio_lucro")
@login_required
def exportar_relatorio_lucro():

    data_inicio = request.args.get("data_inicio")
    data_fim = request.args.get("data_fim")

    hoje = date.today()
    primeiro_dia = hoje.replace(day=1)

    if not data_inicio:
        data_inicio = primeiro_dia.isoformat()
    if not data_fim:
        data_fim = hoje.isoformat()

    with engine.connect() as conn:

        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

        imposto_percent = float(cfg["imposto_percent"] or 0)
        despesas_percent = float(cfg["despesas_percent"] or 0)

        rows = conn.execute(
            select(
                produtos.c.nome.label("produto"),
                func.sum(vendas.c.quantidade).label("qtd"),
                func.sum(vendas.c.receita_total).label("receita"),
                func.sum(vendas.c.custo_total).label("custo"),
                func.sum(vendas.c.margem_contribuicao).label("margem_atual"),
            )
            .select_from(vendas.join(produtos))
            .where(vendas.c.data_venda >= data_inicio)
            .where(vendas.c.data_venda <= data_fim + "T23:59:59")
            .group_by(produtos.c.id)
        ).mappings().all()

    linhas_export = []

    for r in rows:

        receita = float(r["receita"] or 0)
        custo = float(r["custo"] or 0)
        margem_atual = float(r["margem_atual"] or 0)
        comissao_ml = max(0.0, (receita - custo) - margem_atual)

        imposto_val = receita * (imposto_percent / 100.0)
        despesas_val = receita * (despesas_percent / 100.0)
        margem_liquida = receita - custo - comissao_ml - imposto_val - despesas_val

        linhas_export.append({
            "Produto": r["produto"],
            "Quantidade": float(r["qtd"] or 0),
            "Receita": receita,
            "Custo": custo,
            "Comissão ML": comissao_ml,
            "Imposto": imposto_val,
            "Despesas": despesas_val,
            "Lucro Líquido": margem_liquida,
        })

    df = pd.DataFrame(linhas_export)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Lucro")
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"relatorio_lucro_{data_inicio}_a_{data_fim}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    # ----------------------------------------------------------------------------
# LOGIN / LOGOUT
# ----------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    # Evita exibir login para usuário já logado
    if session.get("usuario_id"):
        return redirect(url_for("dashboard"))

    erro = None

    if request.method == "POST":
        usuario = request.form.get("usuario")
        senha = request.form.get("senha")

        # Usuário fixo por enquanto (fácil de evoluir para tabela depois)
        ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
        ADMIN_PASS = os.environ.get("ADMIN_PASS", "123")

        if usuario == ADMIN_USER and senha == ADMIN_PASS:
            session["usuario_id"] = 1
            session["usuario_nome"] = ADMIN_USER
            flash("Login realizado!", "success")
            return redirect(url_for("dashboard"))
        else:
            erro = "Usuário ou senha inválidos."

    return render_template("login.html", erro=erro)


@app.route("/logout")
def logout():
    session.clear()
    flash("Sessão encerrada!", "success")
    return redirect(url_for("login"))
    # Inicialização
init_db()

# Execução
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
