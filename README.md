import sqlite3
from contextlib import closing
from datetime import date
from typing import Any, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:
    import plotly.express as px
except ModuleNotFoundError:  # pragma: no cover - optional dependency fallback
    px = None

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ModuleNotFoundError:  # pragma: no cover - optional dependency fallback
    HAS_STREAMLIT = False

    class _DummyContext:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def metric(self, label: str, value: Any):
            print(f"[METRIC] {label}: {value}")

        def plotly_chart(self, *args, **kwargs):
            print("[INFO] Gráfico não exibido porque Streamlit não está disponível.")

        def dataframe(self, df, **kwargs):
            print("[DATAFRAME]")
            print(df)

        def subheader(self, text: str):
            print(f"\n## {text}")

        def caption(self, text: str):
            print(text)

        def write(self, text: str):
            print(text)

        def markdown(self, text: str):
            print(text)

        def info(self, text: str):
            print(f"[INFO] {text}")

        def error(self, text: str):
            print(f"[ERROR] {text}")

        def success(self, text: str):
            print(f"[SUCCESS] {text}")

    class StreamlitShim:
        """Fallback mínimo para executar o script sem Streamlit instalado."""

        sidebar = _DummyContext()

        @staticmethod
        def set_page_config(**kwargs):
            return None

        @staticmethod
        def title(text: str):
            print(f"\n# {text}")

        @staticmethod
        def caption(text: str):
            print(text)

        @staticmethod
        def header(text: str):
            print(f"\n# {text}")

        @staticmethod
        def subheader(text: str):
            print(f"\n## {text}")

        @staticmethod
        def write(text: Any):
            print(text)

        @staticmethod
        def markdown(text: str):
            print(text)

        @staticmethod
        def info(text: str):
            print(f"[INFO] {text}")

        @staticmethod
        def error(text: str):
            print(f"[ERROR] {text}")

        @staticmethod
        def success(text: str):
            print(f"[SUCCESS] {text}")

        @staticmethod
        def metric(label: str, value: Any):
            print(f"[METRIC] {label}: {value}")

        @staticmethod
        def dataframe(df, **kwargs):
            print(df)

        @staticmethod
        def plotly_chart(*args, **kwargs):
            print("[INFO] Gráfico não exibido porque Streamlit não está disponível.")

        @staticmethod
        def columns(n: int):
            return [_DummyContext() for _ in range(n)]

        @staticmethod
        def expander(label: str, expanded: bool = False):
            print(f"\n### {label}")
            return _DummyContext()

        @staticmethod
        def form(key: str):
            return _DummyContext()

        @staticmethod
        def text_input(label: str, value: str = ""):
            return value

        @staticmethod
        def text_area(label: str, value: str = ""):
            return value

        @staticmethod
        def selectbox(label: str, options: Sequence[Any], index: int = 0):
            return options[index] if options else None

        @staticmethod
        def number_input(label: str, min_value=None, value=0, step=1):
            return value

        @staticmethod
        def date_input(label: str, value=None):
            return value or date.today()

        @staticmethod
        def form_submit_button(label: str):
            return False

        @staticmethod
        def radio(label: str, options: Sequence[Any], index: int = 0):
            return options[index] if options else None

        @staticmethod
        def slider(label: str, min_value=0.0, max_value=1.0, value=0.5, step=0.1):
            return value

        @staticmethod
        def download_button(label: str, data=None, file_name: str = "", mime: str = ""):
            print(f"[INFO] Download disponível em modo Streamlit: {file_name}")

    st = StreamlitShim()

DB_PATH = "locasmart.db"


# =========================
# CONFIG
# =========================
st.set_page_config(
    page_title="LocaSmart MVP",
    page_icon="⚡",
    layout="wide",
)


# =========================
# HELPERS
# =========================
def brl(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def safe_plot(chart_type: str, df: pd.DataFrame, **kwargs):
    """Cria gráfico somente quando Plotly estiver disponível."""
    if px is None:
        return None
    factory = getattr(px, chart_type, None)
    if factory is None:
        return None
    return factory(df, **kwargs)


# =========================
# DATABASE
# =========================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    with closing(get_conn()) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL,
                empresa TEXT,
                telefone TEXT,
                email TEXT,
                cidade TEXT,
                segmento TEXT,
                observacao TEXT,
                data_cadastro TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS equipamentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo TEXT NOT NULL,
                categoria TEXT NOT NULL,
                descricao TEXT,
                potencia_tamanho TEXT,
                tipo TEXT,
                status TEXT NOT NULL,
                custo_medio REAL DEFAULT 0,
                valor_medio REAL DEFAULT 0,
                observacao TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS locacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contrato TEXT NOT NULL,
                cliente_id INTEGER NOT NULL,
                equipamento_id INTEGER NOT NULL,
                cidade TEXT,
                tipo_evento TEXT,
                categoria TEXT NOT NULL,
                quantidade INTEGER NOT NULL,
                data_contrato TEXT,
                data_inicio TEXT NOT NULL,
                data_fim TEXT NOT NULL,
                valor REAL NOT NULL,
                custo REAL DEFAULT 0,
                status TEXT NOT NULL,
                observacao TEXT,
                FOREIGN KEY (cliente_id) REFERENCES clientes(id),
                FOREIGN KEY (equipamento_id) REFERENCES equipamentos(id)
            )
            """
        )
        conn.commit()


def seed_data():
    with closing(get_conn()) as conn:
        cur = conn.cursor()

        clientes_count = cur.execute("SELECT COUNT(*) FROM clientes").fetchone()[0]
        equipamentos_count = cur.execute("SELECT COUNT(*) FROM equipamentos").fetchone()[0]
        locacoes_count = cur.execute("SELECT COUNT(*) FROM locacoes").fetchone()[0]

        if clientes_count == 0:
            cur.executemany(
                """
                INSERT INTO clientes (nome, empresa, telefone, email, cidade, segmento, observacao)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("Carlos Souza", "Eventos Vale", "51999990001", "carlos@eventosvale.com", "Lajeado", "Eventos", "Cliente recorrente"),
                    ("Ana Martins", "Feiras RS", "51999990002", "ana@feirasrs.com", "Santa Cruz do Sul", "Feiras", "Prefere tendas grandes"),
                    ("Prefeitura de Encantado", "Prefeitura", "51999990003", "compras@encantado.rs.gov.br", "Encantado", "Órgão público", "Eventos municipais"),
                ],
            )

        if equipamentos_count == 0:
            cur.executemany(
                """
                INSERT INTO equipamentos (codigo, categoria, descricao, potencia_tamanho, tipo, status, custo_medio, valor_medio, observacao)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("GER-150", "Gerador", "Gerador diesel", "150 kVA", "Diesel", "Disponível", 800, 3500, "Alta procura"),
                    ("GER-250", "Gerador", "Gerador diesel", "250 kVA", "Diesel", "Disponível", 1200, 5200, "Para eventos maiores"),
                    ("TEN-10X10", "Tenda", "Tenda piramidal", "10x10", "Piramidal", "Disponível", 300, 1800, "Mais locada"),
                    ("TEN-05X05", "Tenda", "Tenda piramidal", "5x5", "Piramidal", "Disponível", 180, 900, "Boa margem"),
                ],
            )

        if locacoes_count == 0:
            cur.executemany(
                """
                INSERT INTO locacoes (
                    contrato, cliente_id, equipamento_id, cidade, tipo_evento, categoria,
                    quantidade, data_contrato, data_inicio, data_fim, valor, custo, status, observacao
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("CTR-001", 1, 1, "Lajeado", "Feira", "Gerador", 1, "2026-01-05", "2026-01-10", "2026-01-12", 3500, 900, "Finalizado", "Evento de negócios"),
                    ("CTR-002", 2, 3, "Santa Cruz do Sul", "Show", "Tenda", 2, "2026-01-12", "2026-01-20", "2026-01-22", 3600, 700, "Finalizado", "Estrutura palco"),
                    ("CTR-003", 3, 2, "Encantado", "Evento municipal", "Gerador", 1, "2026-02-01", "2026-02-15", "2026-02-18", 5200, 1300, "Finalizado", "Carnaval"),
                    ("CTR-004", 1, 4, "Lajeado", "Feira", "Tenda", 3, "2026-02-10", "2026-02-25", "2026-02-27", 2700, 600, "Finalizado", "Apoio externo"),
                    ("CTR-005", 2, 1, "Santa Cruz do Sul", "Expo", "Gerador", 1, "2026-03-05", "2026-03-20", "2026-03-23", 3700, 950, "Confirmado", ""),
                ],
            )

        conn.commit()


# =========================
# DATA ACCESS
# =========================
def read_sql(query: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
    with closing(get_conn()) as conn:
        return pd.read_sql_query(query, conn, params=params or ())


def insert_cliente(nome, empresa, telefone, email, cidade, segmento, observacao):
    with closing(get_conn()) as conn:
        conn.execute(
            """
            INSERT INTO clientes (nome, empresa, telefone, email, cidade, segmento, observacao)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (nome, empresa, telefone, email, cidade, segmento, observacao),
        )
        conn.commit()


def insert_equipamento(codigo, categoria, descricao, potencia_tamanho, tipo, status, custo_medio, valor_medio, observacao):
    with closing(get_conn()) as conn:
        conn.execute(
            """
            INSERT INTO equipamentos (codigo, categoria, descricao, potencia_tamanho, tipo, status, custo_medio, valor_medio, observacao)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (codigo, categoria, descricao, potencia_tamanho, tipo, status, custo_medio, valor_medio, observacao),
        )
        conn.commit()


def insert_locacao(contrato, cliente_id, equipamento_id, cidade, tipo_evento, categoria, quantidade, data_contrato, data_inicio, data_fim, valor, custo, status, observacao):
    with closing(get_conn()) as conn:
        conn.execute(
            """
            INSERT INTO locacoes (
                contrato, cliente_id, equipamento_id, cidade, tipo_evento, categoria,
                quantidade, data_contrato, data_inicio, data_fim, valor, custo, status, observacao
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                contrato,
                cliente_id,
                equipamento_id,
                cidade,
                tipo_evento,
                categoria,
                quantidade,
                data_contrato,
                data_inicio,
                data_fim,
                valor,
                custo,
                status,
                observacao,
            ),
        )
        conn.commit()


def get_clientes():
    return read_sql("SELECT * FROM clientes ORDER BY nome")


def get_equipamentos():
    return read_sql("SELECT * FROM equipamentos ORDER BY categoria, codigo")


def get_locacoes():
    return read_sql(
        """
        SELECT
            l.*,
            c.nome AS cliente_nome,
            e.codigo AS equipamento_codigo,
            e.descricao AS equipamento_descricao,
            e.potencia_tamanho
        FROM locacoes l
        JOIN clientes c ON c.id = l.cliente_id
        JOIN equipamentos e ON e.id = l.equipamento_id
        ORDER BY l.data_inicio DESC
        """
    )


# =========================
# UTILS
# =========================
def prepare_locacoes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    for col in ["data_contrato", "data_inicio", "data_fim"]:
        out[col] = pd.to_datetime(out[col], errors="coerce")

    out["margem"] = out["valor"] - out["custo"]
    out["dias_locados"] = (out["data_fim"] - out["data_inicio"]).dt.days.clip(lower=0) + 1
    out["mes_ref"] = out["data_inicio"].dt.to_period("M").astype(str)
    return out


def monthly_series(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["mes_ref", value_col])
    return df.groupby("mes_ref", as_index=False)[value_col].sum()


def avg_forecast(series: pd.Series) -> float:
    return float(series.mean()) if len(series) else 0.0


def naive_forecast(series: pd.Series) -> float:
    return float(series.iloc[-1]) if len(series) else 0.0


def ses_forecast(series: pd.Series, alpha: float = 0.4) -> float:
    if len(series) == 0:
        return 0.0
    s = float(series.iloc[0])
    for value in series.iloc[1:]:
        s = alpha * float(value) + (1 - alpha) * s
    return s


def compute_kpis(df_loc: pd.DataFrame, df_cli: pd.DataFrame, df_eq: pd.DataFrame):
    total_loc = len(df_loc)
    faturamento = float(df_loc["valor"].sum()) if not df_loc.empty else 0.0
    ticket = faturamento / total_loc if total_loc else 0.0
    clientes_ativos = int(df_loc["cliente_id"].nunique()) if not df_loc.empty else 0
    indisponiveis = int((df_eq["status"].fillna("").str.lower() != "disponível").sum()) if not df_eq.empty else 0

    if not df_loc.empty:
        dias_locados = df_loc["dias_locados"].sum()
        dias_periodo = max((df_loc["data_fim"].max() - df_loc["data_inicio"].min()).days + 1, 1)
        ativos = max(len(df_eq), 1)
        taxa_ocupacao = min((dias_locados / (dias_periodo * ativos)) * 100, 100.0)
    else:
        taxa_ocupacao = 0.0

    return {
        "Locações": total_loc,
        "Faturamento": faturamento,
        "Ticket Médio": ticket,
        "Clientes Ativos": clientes_ativos,
        "Indisponíveis": indisponiveis,
        "Taxa de Ocupação": taxa_ocupacao,
    }


def render_chart(fig):
    if fig is None:
        st.info("Plotly não está disponível neste ambiente. O gráfico foi omitido.")
    else:
        st.plotly_chart(fig, use_container_width=True)


# =========================
# PAGES
# =========================
def page_dashboard(df_loc, df_cli, df_eq):
    st.title("⚡ LocaSmart MVP")
    st.caption("Análise inteligente de locações de geradores e tendas")

    kpis = compute_kpis(df_loc, df_cli, df_eq)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Locações", f"{kpis['Locações']}")
    c2.metric("Faturamento", brl(kpis["Faturamento"]))
    c3.metric("Ticket Médio", brl(kpis["Ticket Médio"]))
    c4.metric("Clientes Ativos", f"{kpis['Clientes Ativos']}")
    c5.metric("Indisponíveis", f"{kpis['Indisponíveis']}")
    c6.metric("Ocupação", f"{kpis['Taxa de Ocupação']:.1f}%")

    if df_loc.empty:
        st.info("Cadastre locações para visualizar o dashboard.")
        return

    mensal_loc = monthly_series(df_loc.assign(qtd_loc=1), "qtd_loc")
    mensal_fat = monthly_series(df_loc, "valor")

    g1, g2 = st.columns(2)
    with g1:
        fig = safe_plot("bar", mensal_loc, x="mes_ref", y="qtd_loc", title="Locações por mês")
        render_chart(fig)
    with g2:
        fig = safe_plot("line", mensal_fat, x="mes_ref", y="valor", markers=True, title="Faturamento por mês")
        render_chart(fig)

    g3, g4 = st.columns(2)
    with g3:
        por_categoria = df_loc.groupby("categoria", as_index=False)["valor"].sum()
        fig = safe_plot("pie", por_categoria, names="categoria", values="valor", title="Receita por categoria")
        render_chart(fig)
    with g4:
        top_clientes = df_loc.groupby("cliente_nome", as_index=False)["valor"].sum().sort_values("valor", ascending=False).head(10)
        fig = safe_plot("bar", top_clientes, x="cliente_nome", y="valor", title="Top clientes")
        render_chart(fig)

    st.subheader("Contratos em andamento ou futuros")
    hoje = pd.Timestamp(date.today())
    agenda = df_loc[df_loc["data_fim"] >= hoje][[
        "contrato", "cliente_nome", "equipamento_codigo", "data_inicio", "data_fim", "status", "valor"
    ]].sort_values("data_inicio")
    st.dataframe(agenda, use_container_width=True)


def page_locacoes(df_loc, df_cli, df_eq):
    st.title("📋 Locações")

    with st.expander("Nova locação", expanded=False):
        clientes_map = {row["nome"]: int(row["id"]) for _, row in df_cli.iterrows()}
        equip_map = {f"{row['codigo']} | {row['categoria']} | {row['potencia_tamanho']}": int(row["id"]) for _, row in df_eq.iterrows()}

        with st.form("form_locacao"):
            col1, col2, col3 = st.columns(3)
            contrato = col1.text_input("Contrato")
            cliente_label = col2.selectbox("Cliente", list(clientes_map.keys()) if clientes_map else ["Sem clientes cadastrados"])
            equip_label = col3.selectbox("Equipamento", list(equip_map.keys()) if equip_map else ["Sem equipamentos cadastrados"])

            col4, col5, col6 = st.columns(3)
            cidade = col4.text_input("Cidade")
            tipo_evento = col5.text_input("Tipo de evento")
            categoria = col6.selectbox("Categoria", ["Gerador", "Tenda"])

            col7, col8, col9 = st.columns(3)
            quantidade = col7.number_input("Quantidade", min_value=1, value=1)
            data_contrato = col8.date_input("Data do contrato", value=date.today())
            status = col9.selectbox("Status", ["Agendado", "Confirmado", "Em andamento", "Finalizado", "Cancelado"])

            col10, col11, col12, col13 = st.columns(4)
            data_inicio = col10.date_input("Data início", value=date.today())
            data_fim = col11.date_input("Data fim", value=date.today())
            valor = col12.number_input("Valor da locação", min_value=0.0, value=0.0, step=100.0)
            custo = col13.number_input("Custo estimado", min_value=0.0, value=0.0, step=50.0)

            observacao = st.text_area("Observação")
            submitted = st.form_submit_button("Salvar locação")

            if submitted:
                if not clientes_map or not equip_map:
                    st.error("Cadastre cliente e equipamento antes de lançar uma locação.")
                elif not contrato.strip():
                    st.error("Informe o número do contrato.")
                elif data_fim < data_inicio:
                    st.error("A data final não pode ser anterior à data inicial.")
                else:
                    insert_locacao(
                        contrato=contrato.strip(),
                        cliente_id=clientes_map[cliente_label],
                        equipamento_id=equip_map[equip_label],
                        cidade=cidade.strip(),
                        tipo_evento=tipo_evento.strip(),
                        categoria=categoria,
                        quantidade=int(quantidade),
                        data_contrato=str(data_contrato),
                        data_inicio=str(data_inicio),
                        data_fim=str(data_fim),
                        valor=float(valor),
                        custo=float(custo),
                        status=status,
                        observacao=observacao.strip(),
                    )
                    st.success("Locação cadastrada com sucesso. Recarregue a aplicação para ver a atualização.")

    if df_loc.empty:
        st.info("Nenhuma locação cadastrada.")
        return

    filtro_categoria, filtro_status = st.columns(2)
    categoria = filtro_categoria.selectbox("Filtrar por categoria", ["Todas"] + sorted(df_loc["categoria"].dropna().unique().tolist()))
    status = filtro_status.selectbox("Filtrar por status", ["Todos"] + sorted(df_loc["status"].dropna().unique().tolist()))

    view = df_loc.copy()
    if categoria != "Todas":
        view = view[view["categoria"] == categoria]
    if status != "Todos":
        view = view[view["status"] == status]

    st.dataframe(
        view[[
            "contrato", "cliente_nome", "cidade", "tipo_evento", "categoria", "equipamento_codigo",
            "data_inicio", "data_fim", "valor", "custo", "margem", "status"
        ]],
        use_container_width=True,
    )


def page_clientes(df_loc, df_cli):
    st.title("👥 Clientes")

    with st.expander("Novo cliente", expanded=False):
        with st.form("form_cliente"):
            c1, c2, c3 = st.columns(3)
            nome = c1.text_input("Nome")
            empresa = c2.text_input("Empresa")
            telefone = c3.text_input("Telefone")
            c4, c5, c6 = st.columns(3)
            email = c4.text_input("E-mail")
            cidade = c5.text_input("Cidade")
            segmento = c6.text_input("Segmento")
            observacao = st.text_area("Observação")
            submitted = st.form_submit_button("Salvar cliente")
            if submitted:
                if not nome.strip():
                    st.error("Informe o nome do cliente.")
                else:
                    insert_cliente(nome.strip(), empresa.strip(), telefone.strip(), email.strip(), cidade.strip(), segmento.strip(), observacao.strip())
                    st.success("Cliente cadastrado com sucesso. Recarregue a aplicação para ver a atualização.")

    if df_cli.empty:
        st.info("Nenhum cliente cadastrado.")
        return

    if df_loc.empty:
        st.dataframe(df_cli, use_container_width=True)
        return

    resumo = (
        df_loc.groupby(["cliente_id", "cliente_nome"], as_index=False)
        .agg(locacoes=("id", "count"), faturamento=("valor", "sum"), ticket_medio=("valor", "mean"), margem=("margem", "sum"))
        .sort_values("faturamento", ascending=False)
    )

    a, b, c, d = st.columns(4)
    a.metric("Total clientes", f"{len(df_cli)}")
    b.metric("Clientes ativos", f"{resumo['cliente_id'].nunique()}")
    c.metric("Maior cliente", resumo.iloc[0]["cliente_nome"])
    d.metric("Maior faturamento", brl(float(resumo.iloc[0]["faturamento"])))

    g1, g2 = st.columns(2)
    with g1:
        fig = safe_plot("bar", resumo.head(10), x="cliente_nome", y="faturamento", title="Top clientes por faturamento")
        render_chart(fig)
    with g2:
        fig = safe_plot("bar", resumo.head(10), x="cliente_nome", y="locacoes", title="Top clientes por número de locações")
        render_chart(fig)

    st.dataframe(resumo, use_container_width=True)


def page_equipamentos(df_loc, df_eq):
    st.title("🛠️ Equipamentos")

    with st.expander("Novo equipamento", expanded=False):
        with st.form("form_equipamento"):
            c1, c2, c3 = st.columns(3)
            codigo = c1.text_input("Código")
            categoria = c2.selectbox("Categoria", ["Gerador", "Tenda"])
            status = c3.selectbox("Status", ["Disponível", "Locado", "Manutenção", "Inativo"])

            c4, c5, c6 = st.columns(3)
            descricao = c4.text_input("Descrição")
            potencia_tamanho = c5.text_input("Potência ou tamanho")
            tipo = c6.text_input("Tipo")

            c7, c8 = st.columns(2)
            custo_medio = c7.number_input("Custo médio", min_value=0.0, value=0.0, step=50.0)
            valor_medio = c8.number_input("Valor médio", min_value=0.0, value=0.0, step=100.0)
            observacao = st.text_area("Observação")
            submitted = st.form_submit_button("Salvar equipamento")
            if submitted:
                if not codigo.strip():
                    st.error("Informe o código do equipamento.")
                else:
                    insert_equipamento(codigo.strip(), categoria, descricao.strip(), potencia_tamanho.strip(), tipo.strip(), status, float(custo_medio), float(valor_medio), observacao.strip())
                    st.success("Equipamento cadastrado com sucesso. Recarregue a aplicação para ver a atualização.")

    if df_eq.empty:
        st.info("Nenhum equipamento cadastrado.")
        return

    if df_loc.empty:
        st.dataframe(df_eq, use_container_width=True)
        return

    resumo = (
        df_loc.groupby(["equipamento_id", "equipamento_codigo", "categoria", "potencia_tamanho"], as_index=False)
        .agg(locacoes=("id", "count"), faturamento=("valor", "sum"), margem=("margem", "sum"), dias_locados=("dias_locados", "sum"))
        .sort_values("faturamento", ascending=False)
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total equipamentos", f"{len(df_eq)}")
    c2.metric("Disponíveis", f"{(df_eq['status'].fillna('').str.lower() == 'disponível').sum()}")
    c3.metric("Em manutenção", f"{(df_eq['status'].fillna('').str.lower() == 'manutenção').sum()}")
    c4.metric("Mais rentável", resumo.iloc[0]["equipamento_codigo"])

    g1, g2 = st.columns(2)
    with g1:
        fig = safe_plot("bar", resumo.head(10), x="equipamento_codigo", y="faturamento", color="categoria", title="Top equipamentos por faturamento")
        render_chart(fig)
    with g2:
        fig = safe_plot("bar", resumo.head(10), x="equipamento_codigo", y="locacoes", color="categoria", title="Top equipamentos por locações")
        render_chart(fig)

    st.dataframe(resumo, use_container_width=True)


def page_financeiro(df_loc):
    st.title("💰 Financeiro")
    if df_loc.empty:
        st.info("Nenhuma locação cadastrada para análise financeira.")
        return

    faturamento = df_loc["valor"].sum()
    custo = df_loc["custo"].sum()
    margem = df_loc["margem"].sum()
    ticket = df_loc["valor"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Faturamento total", brl(float(faturamento)))
    c2.metric("Custo total", brl(float(custo)))
    c3.metric("Margem estimada", brl(float(margem)))
    c4.metric("Ticket médio", brl(float(ticket)))

    mensal = df_loc.groupby("mes_ref", as_index=False).agg(valor=("valor", "sum"), custo=("custo", "sum"), margem=("margem", "sum"))
    g1, g2 = st.columns(2)
    with g1:
        fig = safe_plot("line", mensal, x="mes_ref", y=["valor", "custo", "margem"], markers=True, title="Evolução financeira por mês")
        render_chart(fig)
    with g2:
        categoria = df_loc.groupby("categoria", as_index=False).agg(valor=("valor", "sum"), margem=("margem", "sum"))
        fig = safe_plot("bar", categoria, x="categoria", y=["valor", "margem"], barmode="group", title="Receita e margem por categoria")
        render_chart(fig)

    st.dataframe(mensal, use_container_width=True)


def page_previsoes(df_loc):
    st.title("📈 Previsões")
    st.caption("Módulo inicial com Average, Naïve e Suavização Exponencial Simples")

    if df_loc.empty:
        st.info("Cadastre locações para gerar previsões.")
        return

    tipo = st.selectbox("Série para previsão", ["Número de locações", "Faturamento"])
    alpha = st.slider("Alpha do SES", min_value=0.1, max_value=0.9, value=0.4, step=0.1)

    if tipo == "Número de locações":
        serie_df = monthly_series(df_loc.assign(qtd_loc=1), "qtd_loc")
        series = serie_df["qtd_loc"]
        value_label = "qtd_loc"
    else:
        serie_df = monthly_series(df_loc, "valor")
        series = serie_df["valor"]
        value_label = "valor"

    prev_avg = avg_forecast(series)
    prev_naive = naive_forecast(series)
    prev_ses = ses_forecast(series, alpha=alpha)

    c1, c2, c3 = st.columns(3)
    c1.metric("Average", f"{prev_avg:.2f}")
    c2.metric("Naïve", f"{prev_naive:.2f}")
    c3.metric("SES", f"{prev_ses:.2f}")

    proj = pd.DataFrame(
        {
            "metodo": ["Average", "Naïve", "SES"],
            "previsao_proximo_mes": [prev_avg, prev_naive, prev_ses],
        }
    )

    fig = safe_plot("line", serie_df, x="mes_ref", y=value_label, markers=True, title=f"Histórico de {tipo.lower()}")
    render_chart(fig)
    st.dataframe(proj, use_container_width=True)

    st.info(
        "Leitura rápida: o histórico foi carregado e as três previsões foram calculadas. "
        "Para uma comparação mais robusta, a próxima evolução é incluir erro histórico (MAE/RMSE) "
        "e escolher automaticamente o melhor método."
    )


def page_relatorios(df_loc, df_cli, df_eq):
    st.title("🧾 Relatórios")
    st.write("Baixe tabelas-base do MVP em CSV.")

    st.download_button(
        "Baixar locações (CSV)",
        data=df_loc.to_csv(index=False).encode("utf-8"),
        file_name="locacoes.csv",
        mime="text/csv",
    )
    st.download_button(
        "Baixar clientes (CSV)",
        data=df_cli.to_csv(index=False).encode("utf-8"),
        file_name="clientes.csv",
        mime="text/csv",
    )
    st.download_button(
        "Baixar equipamentos (CSV)",
        data=df_eq.to_csv(index=False).encode("utf-8"),
        file_name="equipamentos.csv",
        mime="text/csv",
    )


# =========================
# TESTS
# =========================
def run_self_tests():
    series = pd.Series([10, 20, 30])
    assert avg_forecast(series) == 20.0
    assert naive_forecast(series) == 30.0
    assert round(ses_forecast(series, alpha=0.5), 2) == 22.5

    empty_df = pd.DataFrame(columns=["mes_ref", "valor"])
    grouped = monthly_series(empty_df, "valor")
    assert list(grouped.columns) == ["mes_ref", "valor"]
    assert grouped.empty

    loc_df = pd.DataFrame(
        {
            "valor": [1000.0, 2000.0],
            "custo": [200.0, 500.0],
            "cliente_id": [1, 2],
            "dias_locados": [2, 3],
            "data_inicio": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "data_fim": pd.to_datetime(["2026-01-02", "2026-01-04"]),
        }
    )
    cli_df = pd.DataFrame({"id": [1, 2]})
    eq_df = pd.DataFrame({"status": ["Disponível", "Locado"]})
    kpis = compute_kpis(loc_df, cli_df, eq_df)
    assert kpis["Locações"] == 2
    assert kpis["Faturamento"] == 3000.0
    assert kpis["Clientes Ativos"] == 2
    assert kpis["Indisponíveis"] == 1


# =========================
# APP START
# =========================
def main():
    init_db()
    seed_data()

    clientes_df = get_clientes()
    equipamentos_df = get_equipamentos()
    locacoes_df = prepare_locacoes(get_locacoes())

    with st.sidebar:
        st.header("Menu")
        pagina = st.radio(
            "Navegação",
            [
                "Dashboard",
                "Locações",
                "Clientes",
                "Equipamentos",
                "Financeiro",
                "Previsões",
                "Relatórios",
            ],
        )
        st.markdown("---")
        st.caption(
            "LocaSmart MVP • "
            + ("Streamlit + SQLite" if HAS_STREAMLIT else "modo compatível sem Streamlit")
        )

    if pagina == "Dashboard":
        page_dashboard(locacoes_df, clientes_df, equipamentos_df)
    elif pagina == "Locações":
        page_locacoes(locacoes_df, clientes_df, equipamentos_df)
    elif pagina == "Clientes":
        page_clientes(locacoes_df, clientes_df)
    elif pagina == "Equipamentos":
        page_equipamentos(locacoes_df, equipamentos_df)
    elif pagina == "Financeiro":
        page_financeiro(locacoes_df)
    elif pagina == "Previsões":
        page_previsoes(locacoes_df)
    elif pagina == "Relatórios":
        page_relatorios(locacoes_df, clientes_df, equipamentos_df)


if __name__ == "__main__":
    run_self_tests()
    main()
